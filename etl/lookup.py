import pandas
import re
import sqlalchemy
from os import environ as env
from .utils import connect_to_pg, df_to_pg

conn = connect_to_pg()

def read_file(path):
    """ read an external csv into new postgres table """
    df = pandas.read_csv("{}".format(path))
    return df

class LookupValues(object):
    """Look up Values"""
    def __init__(self, table, lookup_field, file, match_field, method, set_flag):
        """Initialize the thing"""
        self.table = table
        self.lookup_field = lookup_field
        self.match_field = match_field
        self.method = method
        self.set_flag = set_flag
        self.df = read_file(file)
        self.df = self.df[pandas.notnull(self.df["{}".format(self.match_field)])]

    def lookup(self):
        if self.method == "match":
            where = "{} = '{}'"
        elif self.method == "contains":
            where = "{} like '%{}%'"

        if self.set_flag:
            cols_to_update = "{} = '{}', is_anon = 't'"
        else:
            cols_to_update = "{} = '{}'" 

        for index, row in self.df.iterrows():
            query = """update {} set {} where {}""".format(
                self.table, 
                cols_to_update.format(self.lookup_field, row[self.match_field]), 
                where.format(self.lookup_field, row[self.lookup_field])
                )
            print(query)
            conn.execute(sqlalchemy.text(query))