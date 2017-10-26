import re
import odo
from os import environ as env
import os
import sys

import sqlalchemy

from direccion import Address

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
  odo.odo(df, 'postgresql://localhost/{}::{}'.format(env['PG_DB'], table), schema=schema)

def connect_to_pg():
  engine = sqlalchemy.create_engine('postgresql+psycopg2://{}/{}'.format(env['PG_CONNSTR'], env['PG_DB']))
  connection = engine.connect()
  return connection

def exec_psql_query(conn, query, verbose=False):
  if verbose:
    print(query)
  conn.execute(query)

def drop_table_if_exists(conn, table):
  query = "drop table if exists {} cascade".format(table)
  exec_psql_query(conn, query, verbose=True)

def psql_to_geojson(table, outfile='test.json', insr=4326, outsr=4326):
  ogr = "ogr2ogr -f GeoJSON {} -s_srs epsg:{} -t_srs epsg:{} pg:dbname={} {}".format(outfile, insr, outsr, env['PG_DB'], table)
  return os.system(ogr)
  
def add_geom_column(conn, table, geom_col, schema='public', proj=4326, geom_type='Geometry'):
  # add geometry column (if not exists)
  query = "alter table {}.{} add column if not exists {} geometry({}, {});".format(schema, table, geom_col, geom_type, proj)
  exec_psql_query(conn, query, verbose=True)
  # create index on that column (if not exists)
  index = "create index if not exists {}_geom_idx on {}.{} using gist({});".format(table, schema, table, geom_col)
  exec_psql_query(conn, index, verbose=True)

def geocode_addresses(conn, table, add_col='address', geom_col='geom'):
  # get values
  addresses = conn.execute("select {} from {} where {} is null".format(add_col, table, geom_col))
  values = addresses.fetchall()
  # loop through values from column
  for v in values:
    # send to direccion.Address
    g = Address(v[0], notify_fail=True)
    # geocode it
    loc = g.geocode()
    if loc:
      # update to set geom_col equal to point result from direccion
      query = "update {} set {} = ST_SetSRID(ST_MakePoint({}, {}), 4326) where {} = '{}'".format(
        table, 
        geom_col,
        loc['location']['x'],
        loc['location']['y'],
        add_col,
        v[0]
        )
      exec_psql_query(conn, query, verbose=True)
    else:
      pass
  