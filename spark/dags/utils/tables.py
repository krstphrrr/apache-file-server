import pandas as pd
import os
from datetime import datetime
import json
# import geopandas as gpd
from pyproj import CRS

# import project.project as proj
import utils.schema as stools # schema_chooser
import utils.table_utils as tutils # table_create, todict
import utils.dbconfig as dbc #db
# import src.utils.ingester as ing # Ingester

def assemble(tablename):
    dir = json.load(open(file=os.path.normpath(os.path.join(os.getcwd(),"utils","config.json") )))["tall_dir"]
    tall_files = {
        os.path.splitext(i)[0]:os.path.normpath(f"{dir}/{i}") for
            i in os.listdir(dir) if not i.endswith(".xlsx")
            and not i.endswith(".ldb")
            and not i.startswith("~$")
            }
    # if "tblProject" in tablename:
    #     return proj.read_template()



    # FOR JOE: csv encoding
    enc = 'utf-8' if 'dataSoilStability' in tablename else 'cp1252'

    # pulling schemas from xlsx and using them to readcsv
    scheme = tutils.todict(tablename)
    if 'Flux' in tablename:
        # 09-16-2022: horizontalflux table sometimes(?) may not have
        # DateEstablished or ProjectKey, which are expected. Removing them
        # from schema before the csv is created.
        del scheme['DateEstablished']
        del scheme['ProjectKey']
    translated = pg2pandas(scheme)
    if tablename in tall_files.keys():
        tempdf = pd.read_csv(

            tall_files[tablename],
            encoding=enc,
            low_memory=False,
            usecols=scheme.keys(),
            index_col=False,
            dtype = translated,
            parse_dates = date_finder(scheme)

            )

        # adding date loaded in db
        #09-16-2022: HOrizontalflux with dateloadedindb (wrong case and empty)
        # needs to be removed and re-added. easier to change the actual excel
        # but not sustainable
        # if 'Flux' in tablename:
        #     if 'DateLoadedInDB' in tempdf.columns:
        #         del scheme['DateLoadedInDB']
        #         scheme['DateLoadedInDb'] = 'Date'
        #         del tempdf['DateLoadedInDB']

        tempdf = dateloaded(tempdf)
        tempdf = bitfix(tempdf, scheme)


        # adding projectkey value
        if "tblProject" not in tablename:
            prj = proj.read_template()
            project_key = prj.loc[0,"project_key"]
            if 'ProjectKey' not in tempdf.columns and 'project_key' not in tempdf.columns:
                tempdf['ProjectKey'] = project_key
            elif 'project_key' in tempdf.columns and 'ProjectKey' not in tempdf.columns:
                tempdf.rename(columns = {'project_key':'ProjectKey'}, inplace=True)
                tempdf['ProjectKey'] = project_key
            elif 'ProjectKey' in tempdf.columns and 'project_key' not in tempdf.columns:
                tempdf['ProjectKey'] = project_key


        if "dataHeader" in tablename:
            schema = tutils.todict(tablename).keys()
            tempdf.rename(columns={"EcologicalSiteId": "EcologicalSiteID"}, inplace=True)
            tempdf = tempdf.filter(schema)
            tempdf = tempdf.drop_duplicates()
            tempdf = geoenable(tempdf)

        if "dataLPI" in tablename:

            # tempdf.drop(columns=["X"], inplace=True)
            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)
            tempdf = tempdf.drop_duplicates()

        if "dataGap" in tablename:
            #
            #
            tempdf = tempdf.drop_duplicates()
            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)
            #
            # # for i in integers:
            # #     tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
            # tempdf  = tempdf[~tempdf.PrimaryKey.isin(gap)]

        if "dataHeight" in tablename:
            # integers = [ "Measure", "LineLengthAmount", 'PointNbr' , 'Chkbox','ShowCheckbox']


            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)
            tempdf = tempdf.drop_duplicates()
            # for i in integers:
            #     tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')


        if "dataSoilStability" in tablename:

            tempdf = tempdf.drop_duplicates()
            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)


        if "dataSpeciesInventory" in tablename:


            tempdf = tempdf.drop_duplicates()
            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)


        if "geoIndicators" in tablename:
            tempdf.drop_duplicates()
            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)

        if "geoSpecies" in tablename:
            tempdf.drop_duplicates()
            schema = tutils.todict(tablename).keys()
            tempdf = tempdf.filter(schema)


        return tempdf
    else:
        return pd.DataFrame()

def bitfix(df, colscheme):
    for i in df.columns:
        if colscheme[i]=='bit' or colscheme[i]=='Bit' or colscheme[i]=="BIT":
            if df[i].isin(['TRUE','FALSE']).any():
                # 09-15-2022: some bit fields have string 0 in them (object dtype)
                # replacing them with None
                if df[i].isin(['0']).any():
                    df[i] = df[i].apply(lambda x: 0 if (type(x)==str) and ('0' in x) else x)
                df[i] = df[i].apply(lambda x: pd.NA if pd.isna(x)==True else x)
                df[i] = df[i].apply(lambda x: 1 if (type(x)==str) and ("TRUE" in x) else x)
                df[i] = df[i].apply(lambda x: 0 if (type(x)==str) and ("FALSE" in x) else x)

                df[i] = df[i].astype('Int64')


            elif df[i].isin(['Y','N']).any():
                if df[i].isin(['0']).any():
                    df[i] = df[i].apply(lambda x: 0 if (type(x)==str) and ('0' in x) else x)
                df[i] = df[i].apply(lambda x: pd.NA if pd.isna(x)==True else x)
                df[i] = df[i].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
                df[i] = df[i].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
                df[i] = df[i].astype('Int64')


    return df.copy()



def geoenable(df):
    df.drop(columns=["wkb_geometry"], inplace=True)
    tempdf = gpd.GeoDataFrame(df,
                crs =CRS("EPSG:4326"),
                geometry=gpd.points_from_xy(
                    df.Longitude_NAD83,
                    df.Latitude_NAD83
                    ))
    tempdf.rename(columns={'geometry':'wkb_geometry'}, inplace=True)
    return tempdf


def dateloaded(df):
    """ appends DateLoadedInDB and dbkey to the dataframe
    """
    if 'DateLoadedInDb' in df.columns:
        df['DateLoadedInDb'] = df['DateLoadedInDb'].astype('datetime64')
        df['DateLoadedInDb'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        df['DateLoadedInDb'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def pg2pandas(pg_schema):
    trans = {
        "text":"object",
        "varchar": "object",
        "geometry": "object",
        "integer":"Int64",
        "bigint":"Int64",
        "bit":"object",
        "smallint":"Int64",
        "real":"float64",
        "double precision":"float64",
        "numeric":"float64",
        "postgis.geometry":"object",
        "date":"str", #important
        "timestamp":"str" #important,
    }
    return {k:trans[v.lower()] for k,v in pg_schema.items()}

def date_finder(trans):
    return [k for k,v in trans.items() if
        ('date' in trans[k].lower()) or
        ('timestamp' in trans[k].lower())]

def create_command(tablename):
    """
    creates a complete CREATE TABLE postgres statement
    by only supplying tablename
    currently handles: HEADER, Gap
    """

    str = f'CREATE TABLE "{tablename}" '
    str+=field_appender(tablename)

    if "Header" in tablename:
        str = header_fix(str)

    elif "Gap" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "Height" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "LPI" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "SoilStability" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "SpeciesInventory" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "Indicators" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "geoSpecies" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "Project" in tablename:
        str = project_fix(str)
        # str = rid_adder(str)

    elif "DustDeposition" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "HorizontalFlux" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    return str

def set_srid():
    return r"""ALTER TABLE public_test."dataHeader"
                ALTER COLUMN wkb_geometry TYPE postgis.geometry(Point, 4326)
                USING postgis.ST_SetSRID(wkb_geometry,4326);"""

def field_appender(tablename):
    """
    uses schema_chooser to pull a schema for a specific table
    and create a postgres-ready string of fields and field types
    """

    str = "( "
    count = 0
    di = pd.Series(
            stools.schema_chooser(tablename).DataType.values,
            index= stools.schema_chooser(tablename).Field).to_dict()

    for k,v in di.items():
        if count<(len(di.items())-1):
            str+= f'"{k}" {v.upper()}, '
            count+=1
        else:
            str+= f'"{k}" {v.upper()} );'
    return str


def header_fix(str):
    """
    adds primary key constraint
    geometry type fix: public_dev is not postgis enabled; https://stackoverflow.com/a/55408170
    """

    fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT PRIMARY KEY,')
    fix = fix.replace('POSTGIS.GEOMETRY,', 'postgis.GEOMETRY(POINT, 4326),')
    return fix

def nonheader_fix(str):
    """
    adds foreign key constraint
    """

    fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT REFERENCES gisdb.public_test."dataHeader"("PrimaryKey"), ')
    return fix



def project_fix(str):
    """
    adds foreign key contraint
    """
    # fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT PRIMARY KEY,')
    fix = str.replace('"ProjectKey" TEXT, ', '"ProjectKey" TEXT PRIMARY KEY REFERENCES gisdb.public_test."dataHeader"("ProjectKey"), ')

    return fix

def rid_adder(str):
    """
    adds an autoincrement field (row id) and sets it as Primarykey
    """

    fix = str.replace('" ( ', '" ( rid SERIAL PRIMARY KEY,')
    return fix


def height_fix(str):
    fix = str.replace('" ( ', '" ( ri)"')



def batcher(schema):

    tablelist = [
    "dataHeader",
    "dataGap",
    "dataHeight",
    "dataLPI",
    "dataSoilStability",
    "dataSpeciesInventory",
    "geoIndicators",
    "geoSpecies",
    "dataHorizontalFlux",
    "tblProject"
    ]

    def filterpks(df,pkunavailable = None):
        if pkunavailable is not None:
            df  = df[~df.PrimaryKey.isin(pkunavailable)]
        return df

    unavailablepks = {}
    complete={}
    # need to create dataheader first for pk filtering belo
    #
    complete['dataHeader'] = assemble('dataHeader').drop_duplicates()

    print("assembling...")
    for table in tablelist:
        df = assemble(table).drop_duplicates()
        if df.empty:
            pass
        else:
            print(f'finding unavailable primarykeys for {table}...')
            if 'tblProject' not in table:
                unavailablepks[table] = [i for i in df.PrimaryKey.unique() if i not in complete['dataHeader'].PrimaryKey.unique() ]
                df = filterpks(df, unavailablepks[table])
                # df  = df[~df.PrimaryKey.isin(pkunavailable)]
            complete[table] = df
            print(f"assembled {table}!")

    print('finished assembling tables! ingesting..')
    # handling data header if it exists
    # if tutils.tablecheck('dataHeader'):
    #     del complete['dataHeader']
    #     del complete['tblProject']

    for tablename,dataframe in complete.items():
        try:
            d = dbc.db(schema)
            if tutils.tablecheck(tablename, schema):
                if dataframe.empty:
                    print(f"skipping {tablename}")
                else:
                    ing.Ingester.main_ingest(dataframe, tablename, d.str, 10000)
                    print(f" ingested '{tablename}'! (noncreate table route)")
            else:
                if dataframe.empty:
                    print(f"skipping {tablename}")
                else:
                    tutils.table_create(tablename, d.str)
                    ing.Ingester.main_ingest(dataframe, tablename, d.str, 10000)
                    print(f" ingested '{tablename}'! (create table route)")
        except Exception as e:
            print(e)
            d = dbc.db(schema)



def df_check(tbllist):

    for i in tbllist:
        if tbl.assemble(i).empty:
            pass
        else:
            print(f"table {i}: all good")
