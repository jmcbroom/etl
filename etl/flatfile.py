from .utils import df_to_pg, clean_column
import pandas

class Flatfile(object):
    def __init__(self, params):
        self.config = params

    def to_postgres(self):
        if self.config['sys'] == 'swordsolutions':
            path = "/home/gisteam/ExpData"
            filepath = '{}/{}'.format(path, self.config['name'])
            
            # read file into df, clean column names, change null values to None for postgres
            df = pandas.read_csv(filepath, sep="\t", lineterminator="\n", encoding="ISO-8859-1")
            df.rename(columns=lambda x: clean_column(x), inplace=True)
            df.where((pandas.notnull(df)), None)
            print(df.columns)
            
            # send df to postgres table
            schema, table = self.config['destination'].split('.')
            df_to_pg(df, schema, table)
        else:
            pass


