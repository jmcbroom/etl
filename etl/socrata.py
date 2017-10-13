import sodapy, os

soda_token = os.environ['SODA_TOKEN']
soda_user = os.environ['SODA_USER']
soda_pass = os.environ['SODA_PASS']
soda_connection = sodapy.Socrata('data.detroitmi.gov', soda_token, soda_user, soda_pass, timeout=54000)
print(soda_connection)

import sqlalchemy
engine = sqlalchemy.create_engine('postgresql+psycopg2://{}/{}'.format(os.environ['PG_CONNSTR'], os.environ['PG_DB']))
db_connection = engine.connect()

class Socrata(object):
    def __init__(self, params):
        self.config = params
        if params['id']:
            self.id = params['id']
    
    def create_dataset(self):
        columns = [ {"fieldname": v['field'], "name": k, "dataTypeName": v['type'] } for k,v in self.config['columns'].items() ]
        ds = soda_connection.create(
            self.config['name'],
            description = '',
            columns = columns,
            row_identifier = None,
            tags = [],
            category = None
        )
        self.id = ds['id']
        soda_connection.publish(self.id)
        return self.id
    
    def replace(self):
        rows = db_connection.execute("select * from {}".format(self.config['table']))
        replace_payload = [ dict(row) for row in rows ]
        job = soda_connection.replace( self.id, replace_payload )
        return job
