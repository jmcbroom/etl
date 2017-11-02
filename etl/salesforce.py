import simple_salesforce
import pandas
import odo
import sqlalchemy

from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

local_engine = sqlalchemy.create_engine("postgresql://{}/{}".format(env['PG_CONNSTR'], env['PG_DB']))
local_connection = local_engine.connect()

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
