import sodapy, os

soda_token = os.environ['SODA_TOKEN']
soda_user = os.environ['SODA_USER']
soda_pass = os.environ['SODA_PASS']
soda_connection = sodapy.Socrata('data.detroitmi.gov', soda_token, soda_user, soda_pass, timeout=54000)

import sqlalchemy
engine = sqlalchemy.create_engine('postgresql+psycopg2://{}/{}'.format(os.environ['PG_CONNSTR'], os.environ['PG_DB']))

from pprint import pprint

class Socrata(object):
    def __init__(self, params):
        self.config = params
        if params['id']:
            self.id = params['id']
        else:
            self.id = None
    
    def create_dataset(self):
        columns = [ {"fieldname": v['field'], "name": k, "dataTypeName": v['type'] } for k,v in self.config['columns'].items() ]
        if 'row_identifier' in self.config.keys():
            row_identifier = self.config['row_identifier']
        else:
            row_identifier = None
        ds = soda_connection.create(
            self.config['name'],
            description = '',
            columns = columns,
            row_identifier = row_identifier,
            tags = [],
            category = None
        )
        self.id = ds['id']
        soda_connection.publish(self.id)
        return self.id
    
    def update(self):
        db_connection = engine.connect()
        rows = db_connection.execute("select * from {}".format(self.config['table']))
        payload = [ dict(row) for row in rows ]
        print(len(payload))
        pprint(payload[0])
        if self.id == None:
            self.id = self.create_dataset()
            
        if self.config['method'] == 'replace':
            job = soda_connection.replace( self.id, payload )
            return job  
        elif self.config['method'] == 'upsert':
            for i in range(0, len(payload), 20000):
                try:
                    r = soda_connection.upsert(self.id, payload[i:i+20000])
                    print(r)
                except:
                    print("Something went wrong on record {}".format(i))
                    soda_connection.upsert(self.id, payload[i:i+20000])
        db_connection.close()