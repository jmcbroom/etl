import simple_salesforce
from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

# takes a report number, returns a collection of ordered dicts
def getReportDetails(num):
    params = { 'includeDetails': 'false' }
    report = SF.restful('analytics/reports/' + num, params=params)
    
    return report

# takes a list of fields, returns a dict of fields and common name maps
def lookupCols(cols):
    d = {}
    for c in cols:
        # remove object name from front
        k = c.split('.', 1)[-1]
        # remove everything after c for common name
        v = k.split('__c', 1)[0].lower()
        # add a new key value to the dict
        d[k] = v
    
    return d

# example use
report = getReportDetails('00Of1000004PL1oEAG')
fields = report['reportMetadata']['detailColumns']
lookup = lookupCols(fields)
