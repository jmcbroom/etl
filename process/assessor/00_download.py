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

drop_joined_query = "drop table if exists assessor.joined cascade"
ct_joined_query = """
create table assessor.joined as (
select 
        s.objectid, 
        s.parcelno as parcelnum, 
        b.propaddr as address,
        b.zipcode as zip_code,
        b.council as council_district,
        z.zoning_rev, 
        p.owner1,
        p.owner2,
        p.taxpayname as taxpayer_name,
        p.owner_street,
        p.owner_state,
        p.owner_city,
        p.owner_zip,
        p.owner_country,
        p.last_sale_date,
        p.last_sale_price::integer,
        p.last_terms_of_sale,
        p.estimated_true_cash_value::integer,
        b.landvalue::integer as land_value,
        b.landav::integer as land_assessed_value,
        b.bldgav::integer as building_assessed_value,
        b.av::integer as assessed_value,
        b.tv::integer as taxable_value,
        p.improved_value::integer,
        b.sev::integer as sev,
        b.pre::integer as pre,
        b.propclass as property_class,
        p.total_acres,
        p.frontage,
        p."depth",
        p.year_built,
        b.cibbldgno as num_buildings,
        p.num_stories,
        l.legaldesc,
        s.wkb_geometry 
from assessor.shapes s
inner join assessor.zoning z on z.parcelno = s.parcelno
inner join assessor.parcelmaster p on p.parcel_no = s.parcelno
inner join assessor.legals l on l.parcel_no = s.parcelno
inner join assessor.baseattr b on b.parcelno = s.parcelno )
"""

print(drop_joined_query)
local_connection.execute(drop_joined_query)
print(ct_joined_query)
local_connection.execute(ct_joined_query)