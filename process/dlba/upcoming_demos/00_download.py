#!/usr/bin/env python
import simple_salesforce
from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

lookup = {
    'Address__c': 'address',
    'DEMO_Planned_Knock_Down_Date__c': 'demo_date',
    'Socrata_Projected_Knocked_By_Date__c': 'socrata_demo_date',
    'Socrata_Reported_Price__c': 'price',
    'Parcel_ID__c': 'parcel_id',
    'Demo_Contractor_TEXT_ONLY__c': 'contractor_name',
    'ACCT_Latitude__c': 'latitude',
    'ACCT_Longitude__c': 'longitude',
    'Council_District__c': 'council_district',
    'Non_HHF_Commercial_Demo__c': 'commercial_building',
    'Socrata_Price_Type__c': 'price_type',
    'DEMO_NTP_Date__c': 'notice_to_proceed_date',
    'Neighborhood__c': 'neighborhood'
}

query = """
Select {} from Case where
    Socrata_Projected_Knocked_By_Date__c != null
        AND
    Socrata_Reported_Price__c > 0
        AND
    Demo_Contractor_TEXT_ONLY__c <> ''
        AND
    Status='Demo Contracted'
""".format(",".join(lookup.keys()))

res = SF.query_all(query)

import pandas
df = pandas.DataFrame.from_records(res['records'])
df.rename(columns=lookup, inplace=True)
df.drop('attributes', inplace=True, axis=1)

# if demo_date col has null values, fill them with socrata_demo_dates
df['demo_date'].fillna(df['socrata_demo_date'], inplace=True)

# filter out demo_dates that are in the past
today = pandas.to_datetime('today')
df['demo_date'] = pandas.to_datetime(df['demo_date'])
df = df[df['demo_date'] >= today]

import odo
odo.odo(df, 'postgresql://{}@localhost/{}::dlba_upcoming_demos'.format(env['PG_USER'], env['PG_DB']))
