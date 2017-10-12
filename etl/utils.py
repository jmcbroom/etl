import re
import odo
from os import environ as env

def clean_column(column):
  remove = "().?&'#"
  underscore = """\/"""
  for r in remove:
    column = column.replace(r,"")
  for u in underscore:
    column = column.replace(u,"_")
  column = re.sub(r"[\s]{1,}", " ", column)
  column = re.sub(r"\s", "_", column)
  column = re.sub(r"_{2,}", "_", column)
  column = column.rstrip("_ ")
  column = column.lstrip("_ ")
  return column.lower()

def df_to_pg(df, schema, table):
  odo.odo(df, 'postgresql://localhost/{}::{}'.format(env['PG_DB'], table), schema=schema)
  