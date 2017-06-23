import fire
from os import environ as env
from os import unlink, getcwd
from arcgis import gis
from smartsheet import Smartsheet

smartsheet = Smartsheet(env['SMARTSHEET_API_TOKEN'])
root = "https://detroitmi.maps.arcgis.com"
ago = gis.GIS(root, env['AGO_USER'], env['AGO_PASS'])

class SheetToAGO(object):
    def __init__(self, sheet_id=7881523794864004):
        self.sheet_id = sheet_id
        self.sheet = smartsheet.Sheets.get_sheet(self.sheet_id)
        self.title = self.sheet.name

    def sheet_as_csv(self):
        smartsheet.Sheets.get_sheet_as_csv(self.sheet_id, './')

    def sheet_to_ago(self):
        self.sheet_as_csv()
        csv_path = "{}/{}.csv".format(getcwd(), self.title)
        # to-do: flesh this out
        csv_properties= {
            'title': self.title,
            'description':'',
            'tags':''
            }
        # send it up to ArcGIS Online
        ago.content.add(item_properties=csv_properties,
                        data=csv_path,
                        thumbnail = None)
        # remove the CSV
        unlink(csv_path)

if __name__ == "__main__":
    fire.Fire(SheetToAGO)
