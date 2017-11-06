import simple_salesforce
import pandas
import odo
import sqlalchemy
import re

from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

local_engine = sqlalchemy.create_engine("postgresql://{}/{}".format(env['PG_CONNSTR'], env['PG_DB']))
local_connection = local_engine.connect()

def strip_sf_identifiers(name):
    """Remove __r and __c from a field name"""
    name = re.sub("__r\.", "_", name)
    name = re.sub("__c$", "", name)
    return name

def flatten_record(rec):
    new_rec = {}
    for k,v in rec.items():
        # don't copy over 'attributes'
        if k == 'attributes':
            pass
        # if it's a relationship...
        elif k.endswith('__r'):
            try:
                # iterate through the OrderedDict we're given
                for a, b in v.items():
                    # ignore these keys
                    if a not in ['type', 'url', 'attributes']:
                        # construct a new key from the top-level key and the field name
                        key = re.sub("__r$", "", k) + '_' + re.sub("__c$", "", a)
                        # create it
                        new_rec[key] = b
            except AttributeError:
                pass
        # strip off '__c'
        elif k.endswith('__c'):
            new_rec[re.sub("__c$", "", k)] = v
        # no __c? just add it
        else:
            new_rec[k] = v
    return new_rec

class SfTable(object):
  """Class representing a Salesforce table."""

  def __init__(self, params):
    # self.object = params['object']
    self.object = params['object']
    self.destination = re.sub("__(c|r)$", "", params['object']).lower()
    self.fields = params['fields']
    self.schema = params['schema']
    self.query = "Select {} from {}".format(",".join(set(self.fields)), self.object)
    print(self.query)

  def to_postgres(self):
    res = SF.query_all(self.query)
    df = pandas.DataFrame.from_records([ flatten_record(r) for r in res['records'] ])
    df.columns = df.columns.str.lower()
    print(df.columns)
    for f in self.fields:
        if "__r" in f:
            actual = strip_sf_identifiers(f).lower()
            df[actual].replace({None: ''}, inplace=True)

    local_connection.execute("drop table if exists {}.{}".format(self.schema, self.destination))
    
    odo.odo(df, "postgresql://{}@localhost/{}::{}".format(env['PG_USER'], env['PG_DB'], self.destination), schema=self.schema)