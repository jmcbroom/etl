#!/usr/bin/env python
import simple_salesforce
import pandas
import odo
from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])
print('Connected to SF ', SF)

# lookup fields from the dba comm demo object and related property account
lookup = {
    'Name': 'name',
    'Commercial_Demo_Status__c': 'status',
    'BSEED_COM_Final_Grade_Approved__c': 'bseed_com_final_grade_approved',
    'BSEED_COM_Open_Hole_Approved__c': 'bseed_com_open_hole_approved',
    'BSEED_COM_Winter_Grade_Approved__c': 'bseed_com_winter_grade_approved',
    'DBA_Received_EMG_Letter_Date__c': 'dba_received_emg_letter_date',
    'Final_Grade_Approved_Dt__c': 'final_grade_approved_dt',
    'Open_Hole_Approved_Dt__c': 'open_hole_approved_dt',
    'Winter_Grade_Approved_Dt__c': 'winter_grade_approved_dt',
    'Demo_Cost_Abatement__c': 'demo_cost_abatement',
    'Demo_Cost_Knock__c': 'demo_cost_knock',
    'Demo_NtP_Dt__c': 'demo_ntp_dt',
    'Demo_Proj_Demo_Dt__c': 'demo_proj_demo_dt',
    'Demo_Pulled_Date__c': 'demo_pulled_date',
    'ENV_Demo_Proceed_Dt__c': 'env_demo_proceed_dt',
    'ENV_Group_Number__c': 'env_group_number',
    'Knock_Start_Dt__c': 'knock_start_dt',
    'Demolition_Contractor__r.Name': 'demo_contractor',
    'DBA_COM_Property__r.Council_District__c': 'dba_com_property_council_district',
    'DBA_COM_Property__r.Latitude__c': 'dba_com_property_lat',
    'DBA_COM_Property__r.Longitude__c': 'dba_com_property_lng',
    'DBA_COM_Property__r.Name': 'dba_com_property_name',
    'DBA_COM_Property__r.Neighborhood__c': 'dba_com_property_neighborhood',
    'DBA_COM_Property__r.Parcel_ID__c': 'dba_com_property_parcel_id'
}

# query salesforce
query = """
Select {} from DBA_Commercial_Demo__c where
    Knock_Start_Dt__c >= 2014-01-01
        OR
    Knock_Start_Dt__c = Null
""".format(",".join(lookup.keys()))

res = SF.query_all(query)

# make a dataframe with our result
df = pandas.DataFrame.from_records(res['records'])
df.rename(columns=lookup, inplace=True)
df.drop('attributes', inplace=True, axis=1)

print('Created dataframe ', df.shape)

# filter rows by demo status and where demo pull date is null
df = df.loc[df['status'].isin(['Demo Contracted', 'Demolished', 'Demo Pipeline'])]
df = df[df['demo_pulled_date'].isnull()]

print('Filtered dataframe ', df.shape)

# add new cols - get related property account details
df['address'] = df['DBA_COM_Property__r'].apply(lambda x: x['Name'])
df['parcel_id'] = df['DBA_COM_Property__r'].apply(lambda x: x['Parcel_ID__c'])
df['latitude'] = df['DBA_COM_Property__r'].apply(lambda x: x['Latitude__c'])
df['longitude'] = df['DBA_COM_Property__r'].apply(lambda x: x['Longitude__c'])
df['neighborhood'] = df['DBA_COM_Property__r'].apply(lambda x: x['Neighborhood__c'] if x['Neighborhood__c'] else "")
df['council_district'] = df['DBA_COM_Property__r'].apply(lambda x: x['Council_District__c'] if x['Council_District__c'] else "")

# add new col - get related contractor name, account for NoneType
def getContractorName(obj):
    try:
        obj['Name']
        return obj['Name']
    except TypeError:
        return ""

df['demo_contractor_name'] = df['Demolition_Contractor__r'].apply(lambda x: getContractorName(x))

# drop relational cols after creating new ones bc postgres can't process OrderedDicts
del df['DBA_COM_Property__r']
del df['Demolition_Contractor__r']

# add new cols - prioritize bseed inspection approval date, if that's null use demo inspection date
df['open_hole_date'] = df['bseed_com_open_hole_approved'].fillna(df['open_hole_approved_dt'])
df['winter_grade_date'] = df['bseed_com_winter_grade_approved'].fillna(df['winter_grade_approved_dt'])
df['final_grade_date'] = df['bseed_com_final_grade_approved'].fillna(df['final_grade_approved_dt'])

# add new col - sum knock and abatement costs for total demo cost
df['total_demo_cost'] = df['demo_cost_abatement'] + df['demo_cost_knock']

# if already knocked down, zero out projected demo dt
df.loc[df.status.isin(['Demolished']), 'demo_proj_demo_dt'] = None

print('Cleaned data, sending to postgres...')

# send it to postgres
odo.odo(df, 'postgresql://{}@localhost/{}::dlba_comm_demos'.format(env['PG_USER'], env['PG_DB']))
print('Sent to postgres')
