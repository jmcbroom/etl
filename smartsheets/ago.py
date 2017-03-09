import json, requests, re, os, fire
import pandas as pd
from os import environ as env
from arcgis import gis
from smartsheet import Smartsheet

smartsheet = Smartsheet(env['SMARTSHEET_TOKEN'])
ago = gis.GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])

class SheetToAGO(object):
    def __init__(self, sheet_id=7881523794864004):
        self.sheet_id = sheet_id
        self.sheet = smartsheet.Sheets.get_sheet(self.sheet_id)
        self.title = self.sheet.name

    def sheet_as_csv(self):
        res = smartsheet.Sheets.get_sheet_as_csv(self.sheet_id, './')

    def csv_to_featureservice(self):
        self.sheet_as_csv()
        csv_path = "{}/{}.csv".format(os.getcwd(), self.title)
        print(csv_path)
        csv_properties= { 'title': self.title, 'description':'', 'tags':'' }
        ago.content.add(item_properties=csv_properties, data=csv_path, thumbnail = None)
        os.unlink(csv_path)

if __name__ == "__main__":
    fire.Fire(SheetToAGO)
