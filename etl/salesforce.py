import simple_salesforce
from os import environ as env
import fire

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

wheredict = {
    'greaterOrEqual': '>=',
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
    def __init__(self, report_id='00Of1000004PL1oEAG', include_details=False):
        if include_details:
            params = { 'includeDetails': 'true' }
        else:
            params = { 'includeDetails': 'false' }
        self.report = SF.restful("analytics/reports/{}".format(report_id), params=params)
        self.lookup = { clean_sf_col(k): v['label'] for k, v in self.report['reportExtendedMetadata']['detailColumnInfo'].items() }
        self.wheres = [ "{} {} {}".format(clean_sf_col(f['column']), wheredict[f['operator']], f['value']) for f in self.report['reportMetadata']['reportFilters'] ]

    def to_dataframe(self):
        return "Select {} from Case where {}".format(",".join(self.lookup.keys()), " and ".join(self.wheres))
        
if __name__ == "__main__":
    fire.Fire(SfReport)


