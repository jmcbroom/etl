# ref: https://stackoverflow.com/questions/6323860/sibling-package-imports/10131358
if __name__ == "__main__" and __package__ is None:
    from sys import path
    from os.path import dirname as dir

    path.append(dir(path[0]))
    __package__ = "smartsheets"

import esri, fire
from os import environ as env
from smartsheet import Smartsheet

smartsheet = Smartsheet(env['SMARTSHEET_API_TOKEN'])

class EnrichSheetAddresses(object):

    def __init__(self, sheet_id=7881523794864004, address_col='address'):
        self.sheet_id = sheet_id
        self.sheet = smartsheet.Sheets.get_sheet(sheet_id)
        self.name = self.sheet.name
        self.geocoder = esri.Geocoder()
        self.address_col = address_col

    def get_address_index(self):
        for c in self.sheet.columns:
            if c.title == self.address_col:
                return c.index
            else:
                return 0

    def add_columns(self, columns=['_ParcelID', '_Address', '_Latitude', '_Longitude']):
        at_index = self.get_address_index() + 1
        to_add = []
        for c in columns:
            new_col = smartsheet.models.Column({
                'title': c,
                'type': 'TEXT_NUMBER',
                'index': at_index
            })
            to_add.append(new_col)
        result = smartsheet.Sheets.add_columns(self.sheet_id, to_add).result
        self.added_columns = { r.title: r.id for r in result }
        return { r.title: r.id for r in result }

    def geocode_rows(self):
        self.add_columns()
        match_col_id = self.added_columns['_Address']
        pid_col_id = self.added_columns['_ParcelID']
        lat_col_id = self.added_columns['_Latitude']
        lon_col_id = self.added_columns['_Longitude']

        # have to get_sheet again, because we've updated the rows!
        self.sheet = smartsheet.Sheets.get_sheet(self.sheet_id)
        for r in self.sheet.rows:
            address = r.cells[self.get_address_index()].value
            if address == None:
                pass
            result = self.geocoder.geocode(address)
            pid_cell = r.get_column(pid_col_id)
            match_cell = r.get_column(match_col_id)
            lat_cell = r.get_column(lat_col_id)
            lon_cell = r.get_column(lon_col_id)
            if result and result['attributes']['Loc_name'] == 'AddressPointGe':
                pid_cell.value = result['attributes']['User_fld']
                match_cell.value = result['attributes']['Match_addr'][:-7]
                lat_cell.value = str(round(result['location']['y'], 5))
                lon_cell.value = str(round(result['location']['x'], 5))
            # top match wasn't the AddressPoint - get top parcel match
            # maybe call to slack
            else:
                best_match = self.geocoder.match_to_parcel(address, result)
                if best_match:
                    pid_cell.value = best_match['attributes']['parcel_id']
                    match_cell.value = "{} {}".format(best_match['attributes']['house_number'], best_match['attributes']['street_name'])
                    lat_cell.value = str(round(best_match['geometry']['y'], 5))
                    lon_cell.value = str(round(best_match['geometry']['x'], 5))
                else:
                    pass
            if pid_cell.value != None and match_cell.value != None:
                print(r.to_dict())
                r.set_column(match_cell.column_id, match_cell)
                r.set_column(pid_cell.column_id, pid_cell)
                r.set_column(lat_cell.column_id, lat_cell)
                r.set_column(lon_cell.column_id, lon_cell)
                smartsheet.Sheets.update_rows(self.sheet_id, [r])

if __name__ == "__main__":
    fire.Fire(EnrichSheetAddresses)
