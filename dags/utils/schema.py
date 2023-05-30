import pandas as pd
import os
import json
from datetime import datetime
import src.utils.ingester as ing # Ingester
import src.utils.table_utils as tutils
import src.utils.tables as tbl
import src.utils.dbconfig as dbc
import six
# parse types per table
# schema_chooser("aero_runs")


def schema_chooser(tablename, which=0):
    #  PATH TO EXCEL FILE WITH SCHEMA
    schema_dir = json.load(open(file=os.path.normpath(os.path.join(os.getcwd(),"src","utils","config.json") )))["schema_dir"]

    # SCHEMA PATH LOADER
    schema_list = [
        os.path.normpath(f"{schema_dir}/{i}") for i in os.listdir(schema_dir)
        if "Schema" in i
        and os.path.splitext(i)[1].endswith(".xlsx")
        and not i.startswith("~$") ]

    if len(schema_list)>1:
        print("found more than 1 schema file in schema dir;")
        for i in schema_list:
            print(f"pos.{schema_list.index(i)} is \"{i}\"")
    else:
        pass
    schema_file = schema_list[which]
    # create dataframe with path
    excel_dataframe = pd.read_excel(schema_file)
    # quick table name fix
    # excel_dataframe['Table'] = excel_dataframe['Table'].apply(lambda x: x.replace('\xa0', ''))

    # strip whitespace from columns with string values to make them selectable
    for i in excel_dataframe.columns:

        if excel_dataframe.dtypes[i]=="object":
            excel_dataframe[i] = excel_dataframe[i].apply(
                lambda x: str(x).replace('\xa0', '').strip() if
                    (type(x)!='float') and
                    # (type(x)=='int') and
                    (pd.isnull(x)!=True)
                    else x
                )

    # return dataframe
    return excel_dataframe[excel_dataframe['Table']==tablename]

def pandas2pg(pg_schema):
    trans = {
        "object":"text",
        'float64':"numeric",
        'int64':"numeric",
        'datetime64[ns]':"Date",
    }
    return {k:trans[v] for k,v in pg_schema.items()}

def schemaTableCreate(conn, schemaVer):
    """ script to create the new table
    args:
        conn:string = which schema this table is going to be sent to on pg.
        schemaVer:string = which version of the schema (assuming we're going
        to have multiple versions in the same table)

    dependencies:
        datetime
        pd
        os
        tutils
        tbl
        pandas2pg

    """
    tablename = "tblSchema"
    schema_dir = json.load(open(file=os.path.normpath(os.path.join(os.getcwd(),"src","utils","config.json") )))["schema_dir"]

    # SCHEMA PATH LOADER
    schema_list = [
        os.path.normpath(f"{schema_dir}/{i}") for i in os.listdir(schema_dir)
        if "Schema" in i
        and os.path.splitext(i)[1].endswith(".xlsx")
        and not i.startswith("~$") ]

    if len(schema_list)>1:
        print("found more than 1 schema file in schema dir;")
        for i in schema_list:
            print(f"pos.{schema_list.index(i)} is \"{i}\"")
    else:
        pass
    which = 0

    schema_file = schema_list[which]
    # create dataframe with path
    excel_dataframe = pd.read_excel(schema_file,encoding="utf-8")
    # excel_dataframe = excel_dataframe.drop(columns=["Order"])
    excel_dataframe["Version"] = schemaVer
    excel_dataframe["Uploaded"] = datetime.now().date()
    for i in excel_dataframe.columns:
        if excel_dataframe.dtypes[i]=="object":
            excel_dataframe[i] = excel_dataframe[i].apply(lambda x: x.strip() if pd.isna(x)!=True and isinstance(x, six.string_types)==True else x)

    # schema
    str = "( "
    count = 0

    pandas = pd.Series(
            excel_dataframe.dtypes.values.astype('str'),
            index=excel_dataframe.columns).to_dict()

    di = pandas2pg(pandas)

    for k,v in di.items():
        if count<(len(di.items())-1):
            str+= f'"{k}" {v.upper()}, '
            count+=1
        else:
            str+= f'"{k}" {v.upper()} );'
    # str = str.replace('"Order" NUMERIC,', "")
    # print(str)
    str2 = f'CREATE TABLE "tblSchema" '
    str2+=str

    str2 = tbl.rid_adder(str2)
    if tutils.tablecheck(tablename, conn):
        d = dbc.db(f'{conn}')
        ing.Ingester.main_ingest(excel_dataframe, tablename, d.str, 10000)
    else:
        try:
            d = dbc.db(f'{conn}')
            con = d.str
            print("checking fields")
            comm = str2
            cur = con.cursor()
            # return comm
            cur.execute(comm)
            cur.execute(tables.set_srid()) if "Header" in tablename else None
            con.commit()

        except Exception as e:
            print(e)
            d = dbc.db(f'{conn}')
            con = d.str
            cur = con.cursor()

        d = dbc.db(f'{conn}')
        ing.Ingester.main_ingest(excel_dataframe, tablename, d.str, 10000)


def schema_restore_csv(conn):
    """ use backup.csv to restore
    table up in pg.
    #1. read csv (make sure it has new fields in the correct order)
    #2. send to pg

    """
    tablename = "tblSchema"
    schemapath = os.path.normpath(os.path.join(os.getcwd(),"src","schemas","backup.xlsx"))
    schemadf = pd.read_excel(schemapath, encoding="utf-8")
    schemadf["Uploaded"] = pd.to_datetime(schemadf["Uploaded"])
    for i in schemadf.columns:
        if schemadf.dtypes[i]=="object":
            schemadf[i] = schemadf[i].apply(lambda x: x.strip() if pd.isna(x)!=True and isinstance(x, six.string_types)==True else x)

    str = "( "
    count = 0

    pandas = pd.Series(
            schemadf.dtypes.values.astype('str'),
            index=schemadf.columns).to_dict()

    di = pandas2pg(pandas)
    for k,v in di.items():
        if count<(len(di.items())-1):
            str+= f'"{k}" {v.upper()}, '
            count+=1
        else:
            str+= f'"{k}" {v.upper()} );'
    # str = str.replace('"Order" NUMERIC,', "")
    # print(str)
    str2 = f'CREATE TABLE "tblSchema" '
    str2+=str
    str2 = tbl.rid_adder(str2)

    try:
        d = dbc.db(f'{conn}')
        con = d.str
        print("checking fields")
        comm = str2
        cur = con.cursor()
        # return comm
        cur.execute(comm)
        con.commit()
    except Exception as e:
        print(e)
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()
    d = dbc.db(f'{conn}')
    ing.Ingester.main_ingest(schemadf, tablename, d.str, 10000)
