import simple_salesforce
import pandas
import odo
import sqlalchemy

from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

local_engine = sqlalchemy.create_engine("postgresql://{}/{}".format(env['PG_CONNSTR'], env['PG_DB']))
local_connection = local_engine.connect()

def flatten_record(rec):
    """Take a record & flatten out any OrderedDicts"""
    new_rec = {}
    for k,v in rec.items():
        # don't copy over 'attributes'
        if k == 'attributes':
            pass
        # if it's a relationship...
        elif k.endswith('__r'):
            # iterate through the OrderedDict we're given
            for a, b in v.items():
                # ignore these keys
                if a not in ['type', 'url', 'attributes']:
                    # construct a new key from the top-level key and the field name
                    key = k.lower().rstrip('__r') + '_' + a.lower().rstrip('__c')
                    # create it
                    new_rec[key] = b
        # strip off '__c'
        elif k.endswith('__c'):
            new_rec[k.lower().rstrip('__c')] = v
        # no __c? just add it
        else:
            new_rec[k.lower()] = v
    return new_rec

class SfTable(object):
  """Class representing a Salesforce table."""

  def __init__(self, params):
    self.object = params['object']
    self.fields = params['fields']
    self.schema = params['schema']
    self.query = "Select {} from {}".format(",".join(self.fields), self.object)
    print(self.query)

  def to_postgres(self):
    res = SF.query_all(self.query)
    df = pandas.DataFrame.from_records(res['records'])
    df.columns = df.columns.str.lower().str.replace('__c', '')
    df.drop('attributes', inplace=True, axis=1)

    local_connection.execute("drop table if exists {}.{}".format(self.schema, self.object.lower()))
    
    odo.odo(df, "postgresql://{}@localhost/{}::{}".format(env['PG_USER'], env['PG_DB'], self.object.lower()), schema=self.schema)
