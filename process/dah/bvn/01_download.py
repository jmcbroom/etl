import sqlalchemy
from os import environ as env

user = env['DAH_USER']
password = env['DAH_PASS']
host = env['DAH_HOST']
db = env['DAH_DB']

dah_engine = sqlalchemy.create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, db))
dah_connection = dah_engine.connect()
print(dah_connection)
local_pg_engine = sqlalchemy.create_engine('postgresql+psycopg2://gisteam@localhost/etl')
pg_conn = local_pg_engine.connect()
print(pg_conn)

lookup = {
    'payments': 'tblDAHPayments',
    'ztickets': 'tblZTickets',
    'dispadjourn': 'tblDispAdjourn',
    'cityfines': 'tblDAHCityFines',
    'blight_ticket_svc_cost': 'tblDAHBlightTicketServiceCostTransactions',
    'reschedule': 'tblReSchedule',
    'courttime': 'tblCourtTime',
    'agency': 'tblAgency',
    'ordinance': 'tblDAHOrdinance',
    'streets': 'tblStreets',
    'violator_address': 'tblDAHViolatorAddress',
    'state': 'tblState',
    'violator_info': 'tblDAHViolatorInfo',
    'security': 'tblSecurity',
    'country': 'tblDAHCountry',
    'disp_type': 'tblDAHDispType'
    'payment_type': 'tblDAHPaymentType'
}

import pandas as pd, odo
for k, v in lookup.items():
    df = pd.read_sql("select * from SWEETSpower.{}".format(v), dah_connection)
    pg_conn.execute("drop table if exists dah_{}".format(k))
    odo.odo(df, 'postgresql://gisteam@localhost/etl::dah_{}'.format(k))
