import re
from os import environ as env
import os
import sys
import sqlalchemy

def clean_column(column):
  remove = "().?&'#"
  underscore = """\/"""
  for r in remove:
    column = column.replace(r,"")
  for u in underscore:
    column = column.replace(u,"_")
  column = re.sub(r"[\s]{1,}", " ", column)
  column = re.sub(r"\s", "_", column)
  column = re.sub(r"_{2,}", "_", column)
  column = column.rstrip("_ ")
  column = column.lstrip("_ ")
  return column.lower()

def df_to_pg(df, schema, table):
  import odo
  odo.odo(df, 'postgresql://{}/{}::{}'.format(env['PG_CONNSTR'], env['PG_DB'], table), schema=schema)

def connect_to_pg():
  engine = sqlalchemy.create_engine('postgresql+psycopg2://{}/{}'.format(env['PG_CONNSTR'], env['PG_DB']))
  connection = engine.connect()
  return connection

def exec_psql_query(conn, query, verbose=False):
  if verbose:
    print(query)
  conn.execute(sqlalchemy.text(query))

def drop_table_if_exists(conn, table):
  query = "drop table if exists {} cascade".format(table)
  exec_psql_query(conn, query, verbose=True)

def psql_to_geojson(table, outfile='test.json', insr=4326, outsr=4326):
  pg_connstr = "dbname={} host=localhost port={} user={}".format(env['PG_DB'], env['PG_PORT'], env['PG_USER'])
  ogr = "ogr2ogr -f GeoJSON {} -s_srs epsg:{} -t_srs epsg:{} pg:'{}' {}".format(outfile, insr, outsr, pg_connstr, table)
  print(ogr)
  return os.system(ogr)

def psql_to_zipshp(table, outfile='test'):
  pgsql2shp = "pgsql2shp -f {} {} {}".format(outfile, env['PG_DB'], table)
  zip_cmd = "zip {}.zip {}.*".format(outfile, outfile)
  rm = "rm {}.{{cpg,dbf,prj,shp,shx}}".format(outfile)
  for cmd in [pgsql2shp, zip_cmd, rm]:
    print(cmd)
    os.system(cmd)

def add_geom_column(conn, table, geom_col, proj=4326, geom_type='Geometry'):
  # add geometry column (if not exists)
  query = "alter table {} add column if not exists {} geometry({}, {});".format(table, geom_col, geom_type, proj)
  exec_psql_query(conn, query, verbose=True)
  # create index on that column (if not exists)
  index = "create index if not exists {}_geom_idx on {} using gist({});".format(table.replace('.','_'), table, geom_col)
  exec_psql_query(conn, index, verbose=True)