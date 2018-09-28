from .utils import df_to_pg
import pandas

# Make a pandas dataframe from a flatfile that we can send to postgres
class Flatfile(object):
    def __init__(self, params):
        self.config = params

    def to_postgres(self):
        if self.config['source'] == 'swordsolutions':
            path = '/home/gisteam/ExpData'
        else:
            pass

      filename = '{}/{}'.format(path, self.config['name'])
      df = pandas.read_csv(filename, sep='{}', lineterminator='\r', encoding = "ISO-8859-1").format(self.config['delimiter'])
      df = df.where((pandas.notnull(df)), None)
      schema, table = self.config['destination'].split('.')
      df_to_pg(df, schema, table)
