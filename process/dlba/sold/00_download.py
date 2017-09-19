import yaml, simple_salesforce, pandas, odo, sqlalchemy
from os import environ as env

with open('tables.yml', 'r') as f:
    tables = yaml.load(f)

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

# local connection
local_engine = sqlalchemy.create_engine('postgresql://gisteam@localhost/etl')
local_connection = local_engine.connect()

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