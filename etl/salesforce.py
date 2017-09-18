import simple_salesforce, pandas
from os import environ as env
import fire

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

wheredict = {
    'greaterOrEqual': '>=',
    'notEqual': '!=',
    'equals': '=',
    'Yes': 'True'
}

def clean_sf_col(colname):
    split = colname.split('.')
    if colname.endswith("RecordType"):
        return "RecordType.DeveloperName"
    # handle child relationships
    if len(split) == 3:
        split[1] = split[1].replace("__c", "__r")
    return ".".join(split[1:])

class SfReport(object):
    def __init__(self, report_id='00Of1000004ulTn', include_details=True):
        if not include_details:
            params = { 'includeDetails': 'false' }
        else:
            params = { 'includeDetails': 'true' }
        self.report = SF.restful("analytics/reports/{}".format(report_id), params=params)
        self.lookup = { k: v['label'] for k, v in self.report['reportExtendedMetadata']['detailColumnInfo'].items() }
        self.rows = []

    def get_result(self):
        for d in self.report['factMap']['T!T']['rows']:
            r = dict(zip([ self.lookup[k] for k in self.report['reportMetadata']['detailColumns'] ], [ c['value'] for c in d['dataCells'] ]))
            for k, v in r.items():
                if str(type(v)) == "<class 'collections.OrderedDict'>":
                    # this is a HACK
                    r[k] = v['amount']
            self.rows.append(r)

    def to_dataframe(self):
        self.get_result()
        self.df = pandas.DataFrame(self.rows)
        print(self.df)

if __name__ == "__main__":
    fire.Fire(SfReport)


