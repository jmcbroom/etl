import json, requests, re, os, fire
import pandas as pd
from os import environ as env
from arcgis import gis
from smartsheet import Smartsheet

smartsheet = Smartsheet(os.environ['SMARTSHEET_TOKEN'])
ago = gis.GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])

# thanks 2 tom buckley
# ref: https://gist.github.com/tombuckley/3c1eeb56f46904dbb143ac398ea79b40
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

def get_sheet_as_df(sheet_id):
    ss1 = smartsheet.Sheets.get_sheet(sheet_id, page_size=0)
    row_count = ss1.total_row_count
    ss1 = smartsheet.Sheets.get_sheet(sheet_id, page_size=row_count)
    df = get_values(ss1)
    s2 = get_columns(ss1)
    df.columns = s2
    return df

class SheetToAGO(object):
    def __init__(self, sheet_id=7881523794864004):
        self.sheet_id = sheet_id
        self.sheet = smartsheet.Sheets.get_sheet(self.sheet_id)
        self.title = self.sheet.name

    def sheet_as_df(self):
        df = get_sheet_as_df(self.sheet_id)
        print(df.head())
        self.df = df

    # ref: https://developers.arcgis.com/python/guide/accessing-and-creating-your-content/#Importing-data-from-a-pandas-data-frame
    def df_to_featureservice(self):
        self.sheet_as_df()
        sheet_fc = ago.content.import_data(self.df)
        sheet_fc_dict = dict(sheet_fc.properties)
        sheet_json = json.dumps(sheet_fc_dict)
        print(sheet_json)
        sheet_item_props = {
            'title': self.title,
            'description':'Example demonstrating conversion of pandas dataframe object to a GIS item',
            'tags': 'arcgis python api, pandas, csv',
            'text': sheet_json,
            'type':'Feature Collection'
            }
        sheet_item = ago.content.add(sheet_item_props)
        print(sheet_item)

if __name__ == "__main__":
    fire.Fire(SheetToAGO)
