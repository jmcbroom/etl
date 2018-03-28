import sys, os
from .utils import clean_column, df_to_pg
from airtable import Airtable
from pprint import pprint
import pandas

class AirtableTable(object):
    def __init__(self, params):
        self.config = params

    def to_postgres(self):
      table = Airtable(self.config['base'], self.config['table'], api_key=os.environ['AIRTABLE_API_KEY'])
      records = []
      for r in table.get_all():
        rec = {}
        rec['id'] = r['id']
        for f in r['fields']:
          rec[clean_column(f)] = r['fields'][f]
        records.append(rec)
      df = pandas.DataFrame.from_records(records)
      df = df.where((pandas.notnull(df)), None)
      schema, table = self.config['destination'].split('.')

      df_to_pg(df, schema, table)