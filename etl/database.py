import sqlalchemy
import cx_Oracle
import pandas
import odo
from os import environ as env
from etl.utils import df_to_pg

def get_table_as_df(dbTable, connString):
  engine = sqlalchemy.create_engine(connString)
  conn = engine.connect()
  df = pandas.read_sql("select * from {}".format(dbTable), conn)
  return df

def make_db_connection_string(dbType, envPrefix):
  # translate from our YML to SQLAlchemy's connection string prefix
  dbTypeLookup = { 
    'oracle': 'oracle',
    'sql-server': 'mssql+pymssql'
  }
  suffixes = ['USER', 'PASS', 'HOST', 'PORT', 'DB']
  connection_string = dbTypeLookup[dbType] + "://{}:{}@{}:{}/{}".format(*[ env["{}_{}".format(envPrefix, s)] for s in suffixes ])
  return connection_string

class DbTable(object):
  def __init__(self, dbtype, source, destination, prefix):
    self.dbType = dbtype
    self.dbTable = source
    self.prefix = prefix.upper()
    self.schema, self.table = destination.split(".")

  def to_postgres(self):
    self.conn = make_db_connection_string(self.dbType, self.prefix)
    self.df = get_table_as_df(self.dbTable, self.conn)
    df_to_pg(self.df, self.schema, self.table)