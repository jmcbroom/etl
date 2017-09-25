import sqlalchemy, odo, pandas, os, re, math, yaml

from os import environ as env

user = env['ASSESS_USER']
pword = env['ASSESS_PASS']
host = env['ASSESS_HOST']
db = env['ASSESS_DB']

assessor_engine = sqlalchemy.create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, pword, host, db))
assessor_connection = assessor_engine.connect()

local_engine = sqlalchemy.create_engine('postgresql://gisteam@localhost/etl')
local_connection = local_engine.connect()

with open('/home/gisteam/etl/process/assessor/tables.yml', 'r') as f:
    tables = yaml.load(f)

for k, v in tables.items():
    columns = [ "{} as {}".format(c, v[c]) for c in v.keys() ]
    query = "select {} from {}".format(", ".join(columns), k)
    print("Getting {}".format(k))
    df = pandas.read_sql(query, assessor_connection)
    local_connection.execute('drop table if exists assessor.{} cascade'.format(k.lower()))
    print("Inserting {} as {}".format(k, k.lower()))
    odo.odo(df, 'postgresql://{}@localhost/{}::{}'.format(env['PG_USER'], env['PG_DB'], k.lower()), schema='assessor')

drop_sales_joined_query = "drop table if exists assessor.sales_joined"
sales_joined_query = """
create table assessor.sales_joined as (
select 
sa.*,
sh.wkb_geometry
from assessor.sales sa 
inner join assessor.shapes sh 
on sa.parcel_no = sh.parcelno);
"""

print(drop_sales_joined_query)
local_connection.execute(drop_sales_joined_query)
print(sales_joined_query)
local_connection.execute(sales_joined_query)