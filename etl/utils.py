import re
import odo
from os import environ as env

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
  
def add_geom_column(conn, table, geom_col, schema='public', proj=4326, geom_type='Geometry'):
  query = "alter table {}.{} add column if not exists {} geometry({}, {});".format(schema, table, geom_col, geom_type, proj)
  exec_psql_query(conn, query, verbose=True)
  index = "create index if not exists {}_geom_idx on {}.{} using gist({});".format(table, schema, table, geom_col)
  exec_psql_query(conn, index, verbose=True)

def geocode_addresses(conn, table, add_col='address', geom_col='geom'):
  # get values
  addresses = conn.execute("select {} from {} where {} is null".format(add_col, table, geom_col))
  values = addresses.fetchall()
  for v in values:
    g = Address(v[0], notify_fail=True)
    loc = g.geocode()
    query = "update {} set {} = ST_SetSRID(ST_MakePoint({}, {}), 4326) where {} = '{}'".format(
      table, 
      geom_col,
      loc['location']['x'],
      loc['location']['y'],
      add_col,
      v[0]
      )
    exec_psql_query(conn, query, verbose=True)
  