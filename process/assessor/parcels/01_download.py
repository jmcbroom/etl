import sqlalchemy, odo, pandas, os, re, math

user = os.environ['ASSESS_USER']
pword = os.environ['ASSESS_PASS']
host = os.environ['ASSESS_HOST']
db = os.environ['ASSESS_DB']
assessor_engine = sqlalchemy.create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, pword, host, db))
assessor_connection = assessor_engine.connect()
local_engine = sqlalchemy.create_engine('postgresql://gisteam@localhost/etl')
local_connection = local_engine.connect()

df = pandas.read_sql("""select
                        pnum,
                        ownername1, 
                        ownername2,
                        ownerstreetaddr,
                        ownercity,
                        ownerstate,
                        ownerzip,
                        "lastSalePrice",
                        "lastSaleDate" from ParcelMaster""", assessor_connection)
local_connection.execute("drop table if exists assessor.updt")
odo.odo(df, 'postgresql://gisteam@localhost/etl::updt', schema='assessor')

local_connection.execute("drop table if exists assessor.legals")
legal_df = pandas.read_sql("select * from Legals", assessor_connection)
odo.odo(legal_df, 'postgresql://gisteam@localhost/etl::legals', schema='assessor')
