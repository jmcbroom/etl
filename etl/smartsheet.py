import smartsheet
smartsheet = smartsheet.Smartsheet()

import odo
import pandas as pd

from etl.utils import clean_column, df_to_pg

# # thanks tom buckley: https://gist.github.com/tombuckley/3c1eeb56f46904dbb143ac398ea79b40
#
def get_sheet_as_df(sheet_id):
    ss1 = smartsheet.Sheets.get_sheet(sheet_id, page_size=0)
    row_count = ss1.total_row_count
    ss1 = smartsheet.Sheets.get_sheet(sheet_id, page_size=row_count)
    df = get_values(ss1)
    s2 = get_columns(ss1)
    df.columns = s2
    df.rename(columns=lambda x: clean_column(x), inplace=True)
    return df

def get_columns(ss):
    cl = ss.get_columns()
    d3 = cl.to_dict()
    df = pd.DataFrame(d3['data'])
    df = df.set_index('id')
    return df.title

def get_values(ss):
    d = ss.to_dict()
    drows = d['rows']
    rownumber = [x['rowNumber'] for x in drows]
    rows = [x['cells'] for x in drows]
    values = [[x['displayValue'] for x in y] for y in rows]
    return pd.DataFrame(values)

class Smartsheet(object):
  def __init__(self, id='2432809366251396'):
    self.id = id
  
  def to_postgres(self, schema, table):
    self.df = get_sheet_as_df(self.id)
    df_to_pg(self.df, schema, table)