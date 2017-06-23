import sys, os
import yaml
import sodapy
from sqlalchemy import create_engine

# accept a config.yml as first argument
with open('./config.yml', 'r') as f:
    config = yaml.load(f)

conf_cols = config['socrata']['columns']
columns = ["""{} as {}""".format(conf_cols[i]['expression'], i) for i in conf_cols]
end = """from {})""".format(config['table'])
create_view_statement = """create view {} as ( select {} {}""".format(config['socrata']['view'], ",".join(columns), end)

# create engine
if config['backend'] == 'postgres':
    engine = create_engine('postgresql+psycopg2://{}/{}'.format(os.environ['PG_CONNSTR'], os.environ['PG_DB']))

elif config['backend'] == 'mysql':
    engine = create_engine('mysql+pymysql://{}/{}'.format(os.environ['MY_CONNSTR'], os.environ['MY_DB']))

conn = engine.connect()

# drop view and create new view
conn.execute("drop view if exists {}".format(config['socrata']['view']))
conn.execute(create_view_statement)

# set up creds to publish
soda_token = os.environ['SODA_TOKEN']
soda_user = os.environ['SODA_USER']
soda_pass = os.environ['SODA_PASS']

# create Socrata client
client = sodapy.Socrata('data.detroitmi.gov', soda_token, soda_user, soda_pass, timeout=1800)

# create column array for dataset creation
socrata_columns = [
    {"fieldname": c, 
     "name": conf_cols[c]['human'], 
     "dataTypeName": conf_cols[c]['type']} 
    for c in conf_cols]

# set up new dataset
new_dataset = client.create(config['name'],
                            description=config['description'],
                            columns=socrata_columns,
                            row_identifier=config['socrata']['row_identifier'],
                            tags=config['socrata']['tags'],
                            category=config['socrata']['category'])
                            
print("New dataset: https://data.detroitmi.gov/resource/{}".format(new_dataset['id']))

# select all records from view
res = conn.execute("select * from {}".format(config['socrata']['view']))
resultset = []
for row in res:
    resultset.append(dict(row))

# send records to socrata
for i in range(0, len(resultset), 50000):
    try:
        client.upsert(new_dataset['id'], resultset[i:i+50000])
    except:
        print("Something went wrong on record {}".format(i))
        client.upsert(new_dataset['id'], resultset[i:i+50000])

client.publish(new_dataset['id'])
