import pandas as pd
import os
from psycopg2 import sql
import numpy as np
import utils.dbconfig as dbc
import utils.schema as stools
import utils.tables as tables

def table_create(tablename: str, conn:str=None):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """
    # d = dbc.db('maindev')

    try:
        print("checking fields")
        comm = tables.create_command(tablename)
        con = conn
        cur = con.cursor()
        # return comm
        cur.execute(comm)
        cur.execute(tables.set_srid()) if "Header" in tablename else None
        con.commit()

    except Exception as e:
        print(e)
        # d = dbc.db('maindev')
        con = conn
        cur = con.cursor()

def tablecheck(tablename, conn="newtall"):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    if conn=="newtall":
        tableschema = "public_test"
    elif conn=="maindev":
        tableschema = "public_dev"
    else:
        tableschma = "public"
    try:
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()

def todict(tablename, length=None, which=0):
    sche = stools.schema_chooser(tablename,which)
    if length and length==True:
        di = pd.Series(
                sche.Length.values,
                index=sche.Field).to_dict()
        return di
    else:
        di = pd.Series(
                sche.DataType.values,
                index=sche.Field).to_dict()
        return di


def geoind_postingest(conn="newtall"):
    """
    fixes geoindicators after it has been ingested

    """
    tableschema = "public_dev" if conn=="maindev" else "public"

    create_mlra_name = r""" alter table public_dev."geoIndicators"
                            add column mlra_name VARCHAR(200);"""

    update_mlra_name =r""" update public_dev."geoIndicators" as target
                            set mlra_name = src.mlra_name
                            from (
                            	  select geo.mlra_name, dh."PrimaryKey", geo.mlrarsym
                            	  from gis.mlra_v42_wgs84 as geo
                            	  join public_dev."dataHeader" as dh
                            	  on ST_WITHIN(dh.wkb_geometry, geo.geom)
                            	) as src
                            where target."PrimaryKey" = src."PrimaryKey"; """

    create_mlrasym = r""" alter table public_dev."geoIndicators"
                            add column mlrarsym VARCHAR(4);  """

    update_mlrasym = r"""update public_dev."geoIndicators" as target
                            set mlrarsym = src.mlrarsym
                            from (
                            	  select geo.mlra_name, dh."PrimaryKey", geo.mlrarsym
                            	  from gis.mlra_v42_wgs84 as geo
                            	  join public_dev."dataHeader" as dh
                            	  on ST_WITHIN(dh.wkb_geometry, geo.geom)
                            	) as src
                            where target."PrimaryKey" = src."PrimaryKey";"""

    create_nanames = r"""alter table public_dev."geoIndicators"
                             add column na_l1name VARCHAR(100),
                             add column na_l2name VARCHAR(100),
                             add column us_l3name VARCHAR(100),
                             add column us_l4name VARCHAR(100);"""

    update_nanames = r"""update public_dev."geoIndicators" as target
                            set na_l1name = src.na_l1name,
                             na_l2name = src.na_l2name,
                             us_l3name = src.us_l3name,
                             us_l4name = src.us_l4name
                            from
                                (select geo.us_l4name, geo.us_l3name, geo.na_l2name, geo.na_l1name,
                                dh."PrimaryKey"
                            	  from gis.us_eco_level_4 as geo
                            	  join public_dev."dataHeader" as dh
                            	  on ST_WITHIN(dh.wkb_geometry, geo.geom)) as src
                            where target."PrimaryKey" = src."PrimaryKey";"""
                            
    create_state = r"""alter table public_dev."geoIndicators"
                        add column "State" TEXT; """
    update_state = r""" update public_dev."geoIndicators" as target
                        set "State" = src.stusps
                        from (
                        	 select geo.stusps, dh."PrimaryKey"
                        	 from gis.tl_2017_us_state_wgs84 as geo
                        	 join public_dev."dataHeader" as dh
                        	 on ST_WITHIN(dh.wkb_geometry, geo.geom)
                        	) as src
                        where target."PrimaryKey" = src."PrimaryKey";"""
    try:
        d = dbc.db(f'{conn}')
        constring = engine_conn_string('newtall')

        con = d.str
        if tablecheck("geoIndicators"): # table exists
            cur = con.cursor()
            # if field does not exist
            cur.execute(create_mlra_name)
            cur.execute(update_mlra_name)
            # if field does not exist
            cur.execute(create_mlrasym)
            cur.execute(update_mlrasym)
            # if field does not exist
            cur.execute(create_nanames)
            cur.execute(update_nanames)
            # if field does not exist
            cur.execute(create_state)
            cur.execute(update_state)
            #  geotif processing
            gi = gpd.read_postgis('select * from public_dev."geoIndicators";', eng, geom_col="wkb_geometry")
            classes = pd.read_sql('select * from public.modis_classes;', d.str)

            pgdf = extract_modis_values(gi, tif)
            pg = pgdf.copy(deep=True)
            pg.rename(columns={"modis_val":"Value"}, inplace=True)
            final = pg.merge(classes, on="Value", how="inner").filter(["PrimaryKey", "Name"])
            final.to_sql('modis_values', eng, schema='public_dev')


        # if cur.fetchone()[0]:
        #     return True
        # else:
        #     return False

    except Exception as e:
        print(e)
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()




def tableComparison():
    """
    compares a schema file to all the tables up in a postgres instance.

    todo: could use schema/db choosing logic
    args:
    none
    """
    results = {}
    tablelist = [
        "dataHeader",
        "dataGap",
        "dataHeight",
        "dataLPI",
        "dataSoilStability",
        "dataSpeciesInventory",
        "geoIndicators",
        "geoSpecies",
        "tblProject"
    ]

    for table in tablelist:
        sch_pg ="sch_missing_in_pg"
        pg_sch = "pg_missing_in_sch"

        schema2compare = tutils.todict(table)
        pg_tbl = pgTableSchema(table,"newtall")
        results[f'{table}_{sch_pg}'] = [i for i in schema2compare.keys() if i not in pg_tbl.keys()]
        results[f'{table}_{pg_sch}'] = [i for i in pg_tbl.keys() if i not in schema2compare.keys()]
    return results


def pgTableSchema(tablename, conn):
    """
    pulls the schema of a table in postgres, renders it in a dictionary

    args:
    tablename:str ->  name of table
    conn:str -> name of section in database.ini, which db/schema to connect to

    """
    empty_dict = {}
    tableschema = "public_test" if conn=="newtall" else "public_dev"
    try:
        d = dbc.db(f"{conn}")
        con = d.str
        cur = con.cursor()
        cur.execute("select column_name, data_type from information_schema.columns where table_name=%s and table_schema=%s;", (f'{tablename}',f'{tableschema}',))
        tuples = cur.fetchall()
        for i in tuples:
            empty_dict[f'{[j for j in i][0]}']=f'{[j for j in i][1]}'
        return empty_dict

    except Exception as e:
        d = dbc.db(f"{conn}")
        con = d.str
        cur = con.cursor()
