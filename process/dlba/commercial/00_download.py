#!/usr/bin/env python
import simple_salesforce
import pandas
import odo
from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])
print('Connected to SF ', SF)

lookup = {
    'Address__c': 'address',
    'Demo_Contractor_TEXT_ONLY__c': 'contractor_name',
    'Socrata_Projected_Knocked_By_Date__c': 'socrata_demo_date',
    'DEMO_Planned_Knock_Down_Date__c': 'planned_demo_date',
    'DEMO_Knock_Down_Date__c': 'demo_date',
    'Socrata_Reported_Price__c': 'price',
    'DEMO_Open_Hole_Approved_Date__c': 'demo_open_hole',
    'BSEED_Open_Hole_Approved__c': 'bseed_open_hole',
    'DEMO_Winter_Grade_Approved_Date__c': 'demo_winter_grade',
    'BSEED_Winter_Grade_Approved__c': 'bseed_winter_grade',
    'DEMO_Final_Grade_Approved_Date__c': 'demo_final_grade',
    'BSEED_Final_Grade_Approved__c': 'bseed_final_grade',
    'Status': 'status',
    'CaseNumber': 'case_no',
    'Parcel_ID__c': 'parcel_id',
    'RecordTypeID': 'record_type_id',
    'Non_HHF_Commercial_Demo__c': 'comm_demo',
    'DBA_Com_ENV_Group_NumText__c': 'env_group_no',
    'Council_District__c': 'council_district',
    'Neighborhood__c': 'neighborhood',
    'ACCT_Latitude__c': 'latitude',
    'ACCT_Longitude__c': 'longitude'
}

# query salesforce
query = """
Select {} from Case where
    DEMO_Knock_Down_Date__c >= 2014-01-01
        AND
    Non_HHF_Commercial_Demo__c='Yes'
""".format(",".join(lookup.keys()))

res = SF.query_all(query)

# make a dataframe from our results
df = pandas.DataFrame.from_records(res['records'])
df.rename(columns=lookup, inplace=True)
df.drop('attributes', inplace=True, axis=1)

print('Created dataframe ', df.shape)

# create new cols - prioritize bseed inspection date, if that's null use demo inspection date
df['open_hole_date'] = df['bseed_open_hole'].fillna(df['demo_open_hole'])
df['winter_grade_date'] = df['bseed_winter_grade'].fillna(df['demo_winter_grade'])
df['final_grade_date'] = df['bseed_final_grade'].fillna(df['demo_final_grade'])

# send it to postgres
odo.odo(df, 'postgresql://{}@localhost/{}::dlba_comm_demos'.format(env['PG_USER'], env['PG_DB']))
print('Sent to postgres')
