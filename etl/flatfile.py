from .utils import df_to_pg, clean_column
import pandas

# Make a pandas dataframe from a flatfile that we can send to postgres
class Flatfile(object):
    def __init__(self, params):
        self.config = params

    def to_postgres(self):
        if self.config['sys'] == 'swordsolutions':
            path = "/home/gisteam/ExpData"
            delimiter = "/t"
            lineterminator = "/n"
            encoding = "ISO-8859-1"
            
            filename = '{}/{}'.format(path, self.config['name'])
            df = pandas.read_csv(filename, sep="\t", lineterminator="\n", encoding="ISO-8859-1")
            df.rename(columns=lambda x: clean_column(x), inplace=True)
            print(df.columns)
            df = df.where((pandas.notnull(df)), None)
            schema, table = self.config['destination'].split('.')
            df_to_pg(df, schema, table)
        else:
            pass


