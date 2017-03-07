import os
from smartsheet import Smartsheet
from sodapy import Socrata
sheet_client = Smartsheet(os.environ['SMARTSHEET_TOKEN'])
socrata_client = Socrata("data.detroitmi.gov", os.environ['SODA_TOKEN'], os.environ['SODA_USER'], os.environ['SODA_PASS'])

def clean_field(field):
    field = field.replace(' ','_')
    field = field.replace('?','_')
    return field.lower()

class SheetToSocrata(object):

    def __init__(self, sheet_id=1851354277799812, socrata_id=None):
        sheet = sheet_client.Sheets.get_sheet(sheet_id)
        self.columns = [c.to_dict() for c in sheet.columns]
        self.smartsheet_cols = { c['id']: c['title'] for c in self.columns }
        self.rows = [r.to_dict() for r in sheet.rows]
        self.name = sheet.name
        self.four_by_four = socrata_id
        self.description = ""

    def create_columns(self):
        fields = []
        # this will need building out
        type_map = {
            'TEXT_NUMBER': 'text',
            'PICKLIST': 'text',
            'CONTACT_LIST': 'text',
            'DATE': 'date',
            'DATETIME': 'date'
        }
        for c in self.columns:
            entry = {}
            entry['fieldName'] = clean_field(c['title'])
            entry['name'] = c['title']
            entry['dataTypeName'] = type_map[c['type']]
            fields.append(entry)
        return fields

    def create_dataset(self):
        cols = self.create_columns()
        response = socrata_client.create(self.name, description=self.description, columns=cols)
        self.four_by_four = response['id']
        self.socrata_cols = { c['fieldName']: c['name'] for c in response['columns'] }
        return "https://data.detroitmi.gov/datasets/{}".format(response['id'])

    def replace_data(self):
        data = []
        for r in self.rows:
            this_row = {}
            for c in r['cells']:
                if c['value']:
                    this_row[self.smartsheet_cols[c['columnId']]] = c['value']
                elif c['displayValue']:
                    this_row[self.smartsheet_cols[c['columnId']]] = c['displayValue']
                else:
                    pass
            data.append(this_row)
        return socrata_client.replace(self.four_by_four, data)

    def publish_dataset(self):
        return socrata_client.publish(self.four_by_four)

class GeocodeAddressColumn(object):

    def __init__(self, sheet_id=18101942039409):
        pass
