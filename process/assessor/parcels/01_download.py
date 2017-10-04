import sqlalchemy, odo, pandas, os, re, math

user = os.environ['ASSESS_USER']
pword = os.environ['ASSESS_PASS']
host = os.environ['ASSESS_HOST']
db = os.environ['ASSESS_DB']

assessor_engine = sqlalchemy.create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, pword, host, db))
assessor_connection = assessor_engine.connect()
local_engine = sqlalchemy.create_engine('postgresql://gisteam@localhost/etl')
local_connection = local_engine.connect()

tables = ["ParcelMaster", "Legals", "Sales"]

for t in tables:
  df = pandas.read_sql("select * from {}".format(t))
  local_connection.execute("drop table if exists assessor.{} cascade".format(t.lower())
  odo.odo(df, "postgresql://gisteam@localhost/etl::{}".format(t.lower()), schema="assessor")
