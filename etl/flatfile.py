from .utils import df_to_pg
import pandas

# Make a pandas dataframe from a flatfile that we can send to postgres
class Flatfile(object):
    def __init__(self, params):
        self.config = params

    def to_postgres(self):
        if self.config['source'] == 'swordsolutions':
            path = "/home/gisteam/ExpData"
            delimiter = "/t"
            lineterminator = "/n"
            encoding = "ISO-8859-1"
        else:
            pass

      filename = '{}/{}'.format(path, self.config['name'])
      df = pandas.read_csv(filename, sep=delimiter, lineterminator=lineterminator, encoding=encoding)
      df = df.where((pandas.notnull(df)), None)
      schema, table = self.config['destination'].split('.')
      df_to_pg(df, schema, table)
