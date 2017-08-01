import os, re, fire
from smartsheet import Smartsheet
from sodapy import Socrata
sheet_client = Smartsheet(os.environ['SMARTSHEET_API_TOKEN'])
socrata_client = Socrata("data.detroitmi.gov", os.environ['SODA_TOKEN'], os.environ['SODA_USER'], os.environ['SODA_PASS'])

def clean_field(field):
    # regex out bad characters
    field = re.sub('[^A-Za-z0-9_]+', '_', field)
    return field.lower().strip('_')

class SheetToSocrata(object):
    def __init__(self, sheet_id=37522882488196, socrata_4x4=None):

        # get a Sheet object from smartsheets
        sheet = sheet_client.Sheets.get_sheet(sheet_id)

        # get an array of the sheet's columns
        self.columns = [c.to_dict() for c in sheet.columns]
        # lookup table for Smartsheet columns
        self.smartsheet_cols = { c['id']: c['title'] for c in self.columns }

        # get an array of the sheet's rows
        self.rows = [r.to_dict() for r in sheet.rows]
        # get the sheet name
        self.name = sheet.name

        # 4x4 and url if we have it
        if socrata_4x4:
            self.4x4 = socrata_4x4
            self.socrata_url = "https://data.detroitmi.gov/datasets/{}".format(self.4x4)
        else:
            self.4x4 = None
            self.socrata_url = None

        # figure this out
        self.description = ""
        # row identifier
        self.unique_id_row = None

    def create_columns(self):
        """Create a column object to give to create_dataset"""
        fields = []

        # to-do: build out this lookup table
        # keys are Smartsheet column types
        # values are Socrata column types
        type_map = {
            'TEXT_NUMBER': 'text',
            'PICKLIST': 'text',
            'CONTACT_LIST': 'text',
            'CHECKBOX': 'text',
            'DATE': 'date',
            'DATETIME': 'date'
        }

        # loop through Smartsheet column array
        for c in self.columns:
            print(c)
            # if this column is the auto-number Unique ID, set that as the row identifier
            if c['systemColumnType'] == 'AUTO_NUMBER':
                self.unique_id_row = clean_field(c['title'])

            # build up a dict to append to fields
            entry = {}
            entry['fieldName'] = clean_field(c['title'])
            entry['name'] = c['title']
            entry['dataTypeName'] = type_map[c['type']]
            fields.append(entry)

        return fields

    def create_dataset(self):
        """Creates an empty working copy in Socrata from Smartsheet schema"""
        # get a column object
        column_obj = self.create_columns()
        # create the dataset
        response = socrata_client.create(self.name, description=self.description, columns=column_obj, row_identifier=self.unique_id_row)
        # get the freshly-created 4x4
        self.4x4 = response['id']
        # create a lookup table for Socrata columns
        self.socrata_cols = { c['fieldName']: c['name'] for c in response['columns'] }
        # create the dataset URL
        self.socrata_url = "https://data.detroitmi.gov/datasets/{}".format(response['id'])
        return self.socrata_url

    def load_data(self, method='upsert'):
        """Loads rows into Socrata"""
        # empty container to hold transformed rows
        data = []
        # loop through array of Smartsheet rows
        for r in self.rows:
            # build up dict to add to data
            this_row = {}
            # for each cell in the row:
            for c in r['cells']:
                if c['value']:
                    # lookup the Smartsheet columnId to get the field name and assign to row dict
                    this_row[self.smartsheet_cols[c['columnId']]] = c['value']
                # sometimes the data is a displayValue and not a value (dates)
                elif c['displayValue']:
                    this_row[self.smartsheet_cols[c['columnId']]] = c['displayValue']
                else:
                    pass
            data.append(this_row)
        if method == 'upsert':
            return socrata_client.upsert(self.4x4, data)
        elif method == 'replace':
            return socrata_client.replace(self.4x4, data)

    def publish_dataset(self):
        return socrata_client.publish(self.4x4)

if __name__ == "__main__":
    fire.Fire(SheetToSocrata)
