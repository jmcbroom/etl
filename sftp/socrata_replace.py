import sys, os
import yaml
import sodapy
from sqlalchemy import create_engine
from slack import SlackMessage
from time import time

soda_token = os.environ['SODA_TOKEN']
soda_user = os.environ['SODA_USER']
soda_pass = os.environ['SODA_PASS']

# create Socrata client
client = sodapy.Socrata('data.detroitmi.gov', soda_token, soda_user, soda_pass, timeout=1800)

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
    conn = engine.connect()
elif config['backend'] == 'mysql':
    engine = create_engine('mysql+pymysql://{}/{}'.format(os.environ['MY_CONNSTR'], os.environ['MY_DB']))
    conn = engine.connect()

# drop view if exists and create new one
conn.execute("drop view if exists {}".format(config['socrata']['view']))
conn.execute(create_view_statement)

res = conn.execute("select * from {}".format(config['socrata']['view']))

# get number of rows in view
count_query = conn.execute("select count(*) from {}".format(config['socrata']['view']))
count = int(count_query.fetchone()[0])

# get number of rows currently on socrata
socrata_count = client.get(config['socrata']['id'], select='count({}) as count'.format(config['socrata']['row_identifier']))

# tell slack we're starting
begin_msg_data = {
  'text': "Replacing dataset: {}\nNumber of rows in Socrata dataset: *{}*\nNumber of rows to replace: *{}*".format(config['name'], socrata_count[0]['count'], count),
  'attachments': [{'text': 'https://data.detroitmi.gov/resource/{}'.format(config['socrata']['id'])}]
  }

count_msg = SlackMessage(begin_msg_data)
count_msg.send()

# start a timer
start_time = time()

# send the records from our db view to socrata
resultset = []
for row in res:
    resultset.append(dict(row))

client.replace(config['socrata']['id'], resultset)
 
# stop the timer       
end_time = time()
seconds = end_time - start_time
m, s = divmod(seconds, 60)
duration = "%02d:%02d" % (m, s)

# get number of rows now on socrata
new_socrata_count = client.get(config['socrata']['id'], select='count({}) as count'.format(config['socrata']['row_identifier']))

# tell slack we're done
completed_msg_data = {
  'text': "Replacement finished in {}.\nNumber of rows in Socrata dataset: *{}*".format(duration, new_socrata_count[0]['count']),
  'attachments': [{'text': 'https://data.detroitmi.gov/resource/{}'.format(config['socrata']['id'])}]
  }

end_msg = SlackMessage(completed_msg_data)
end_msg.send()
