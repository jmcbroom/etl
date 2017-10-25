import sqlalchemy
import cx_Oracle
import pandas
import odo
from os import environ as env

def get_table_as_df(dbType, dbTable, connString):
  if db_type == 'oracle':
    engine = sqlalchemy.create_engine('{}'.format(connString))
    conn = engine.connect()
    res = conn.execute('select * from {}'.format(table))
    df = pandas.DataFrame(res.fetchall())
    df.columns = res.keys
  elif db_type == 'sqlserver':
    pass

  return df

def make_db_connection_string(dbType, envPrefix):
  connection_string = dbtype + "://{}:{}@{}:{}/{}.format(env['" + envPrefix + "_USER'], env['" + envPrefix + "_PASS'], env['" + envPrefix + "_HOST'], env['" + envPrefix + "_PORT'], env['" + envPrefix + "_DB'])"

  return connection_string

class Database(object):
  def __init__(self, db_type, table, prefix):
    self.db_type = db_type
    self.table = table
    self.prefix = prefix

  def to_postgres(self):
  	self.conn = make_db_connection_string(self.db_type, self.prefix)
  	self.df = get_table_as_df(self.db_type, self.table, self.conn)

  	odo.odo(df, "postgres://{}@localhost/{}::{}".format(env['PG_USER'], env['PG_DB'], self.table))
