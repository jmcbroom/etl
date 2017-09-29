from os import environ as env
import yaml, simple_salesforce, sqlalchemy

with open('/home/gisteam/etl/process/dlba/tables.yml', 'r') as f:
    tables = yaml.load(f)

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

# local connection
local_engine = sqlalchemy.create_engine('postgresql://gisteam@localhost/etl')
local_connection = local_engine.connect()

print(local_connection)

def clean_sf_col(colname):
    if colname[-3:] == '__c':
        colname = colname[:-3]
    return colname.lower()

for k, v in tables.items():
    print("Getting {}".format(k))
    # cols = [ "{} as {}".format(col, clean_sf_col(col)) for col in v ]
    query = "Select {} from {}".format(",".join(v), k)
    res = SF.query_all(query)

    df = pandas.DataFrame.from_records(res['records'])
    df.columns = df.columns.str.lower().str.replace('__c','')
    df.drop('attributes', inplace=True, axis=1)

    local_connection.execute('drop table if exists dlba.{} cascade'.format(clean_sf_col(k)))

    print("Inserting {}".format(k))
    odo.odo(df, 'postgresql://{}@localhost/{}::{}'.format(env['PG_USER'], env['PG_DB'], clean_sf_col(k)), schema='dlba')

# Side Lots
sidelot_dt_statement = """drop table if exists dlba.side_lots cascade"""
sidelot_ct_statement = """
create table dlba.side_lots as (
select 
a.actual_closing_date,
a.sale_status, 
c.address, 
c.parcel_id,
c.program, 
c.neighborhood, 
c.council_district, 
c.acct_latitude, 
c.acct_longitude, 
pb.buyer_status,
pb.final_sale_price,
pb.purchaser_type
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
where a.recordtypeid = '012j0000000xtGvAAI' 
and a.actual_closing_date is not null
and pb.buyer_status = 'Selected'
and c.address not like '%Fake St%' )
"""
local_connection.execute(sqlalchemy.text(sidelot_dt_statement.replace("\n", ' ')))
local_connection.execute(sqlalchemy.text(sidelot_ct_statement.replace("\n", ' ')))

# Own It Now
ownitnow_dt_statement = """
drop table if exists dlba.own_it_now cascade
"""

ownitnow_ct_statement = """
create table dlba.own_it_now as (
select 
a.actual_closing_date,
a.sale_status, 
c.address, 
c.parcel_id,
c.program, 
c.neighborhood, 
c.council_district, 
c.acct_latitude, 
c.acct_longitude, 
pb.buyer_status,
pb.final_sale_price,
pb.purchaser_type
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
where a.dlba_activity_type in ('Demo Pull Sale','Demo Pull for Demo Sale','Own It Now','Own It Now - Bundled Property')
and a.actual_closing_date is not null
and pb.buyer_status = 'Selected'
and c.address not like '%Fake St%' )
"""

local_connection.execute(sqlalchemy.text(ownitnow_dt_statement.replace("\n", ' ')))
local_connection.execute(sqlalchemy.text(ownitnow_ct_statement.replace("\n", ' ')))


# Auction Sold
auction_sold_dt_statement = """
drop table if exists dlba.auction_sold cascade
"""

auction_sold_ct_statement = """
create table dlba.auction_sold as (
select 
a.actual_closing_date,
a.sale_status, 
c.address, 
c.parcel_id,
'Auction'::text as program, 
c.neighborhood, 
c.council_district, 
c.acct_latitude, 
c.acct_longitude, 
pb.buyer_status,
pb.final_sale_price,
pb.purchaser_type
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
where a.recordtypeid = '012j0000000xtGoAAI' 
and a.sale_status = 'Closed'
and pb.buyer_status = 'Selected'
and c.address not like '%Fake St%')
"""

local_connection.execute(sqlalchemy.text(auction_sold_dt_statement.replace("\n", ' ')))
local_connection.execute(sqlalchemy.text(auction_sold_ct_statement.replace("\n", ' ')))


# For Sale
forsale_dt_statement = """
drop table if exists dlba.for_sale cascade
"""

forsale_ct_statement = """
create table dlba.for_sale as (
select
c.address,
c.parcel_id,
c.program,
a.listing_date,
c.neighborhood,
c.council_district,
c.acct_latitude,
c.acct_longitude
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
where (a.recordtypeid = '012j0000000xtGoAAI'
or a.dlba_activity_type in ('Demo Pull Sale', 'Demo Pull for Demo Sale', 'Renovation Sale', 'Own It Now', 'Own It Now - Bundled Property', 'Auction - Bundled Property'))
and a.listing_date::timestamp < now() + interval '1 day'
and a.sale_status = 'For Sale On Site'
and c.status = 'For Sale'
and c.address not like '%Fake St%')
"""

local_connection.execute(sqlalchemy.text(forsale_dt_statement.replace("\n", ' ')))
local_connection.execute(sqlalchemy.text(forsale_ct_statement.replace("\n", ' ')))
