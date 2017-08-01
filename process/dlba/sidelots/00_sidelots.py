import pandas
from simple_salesforce import Salesforce
from os import environ as env
SF = Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])

sidelot_lookup = {
    'Id': 'sf_id',
    'Property__c': 'property',
    'ClosedDate': 'closing_date',
    'Address__c': 'address',
    'Parcel_ID__c': 'parcel_id',
    'ACCT_Latitude__c': 'lat',
    'ACCT_Longitude__c': 'lon'
}

query = "Select {} from Case where Sold_Side_Lot__c = True".format(",".join(sidelot_lookup.keys()))
q = SF.query_all(query)

sidelot_df = pandas.DataFrame.from_dict(q['records'])
# drop attributes
sidelot_df.drop(['attributes'], axis=1, inplace=True)
# rename columns - need to reverse the above dict to do this
sidelot_df.rename(columns=sidelot_lookup, inplace=True)

buyer_lookup = {
    "Id": "id",
    "Property__c": "address",
    "Adjacent_Parcel__c": "adjacent_parcel",
    "Final_Sale_Price__c": "sale_price",
    "Deed_Recording_Name__c": "deed_name"
}

buyer_query = "Select {} from Prospective_Buyer__c where Sale_Type__c = 'Side Lot' and Status_Closing__c = 'Closed'".format(",".join(buyer_lookup.keys()))
bq = SF.query_all(buyer_query)

buyer_df = pandas.DataFrame.from_dict(bq['records'])
buyer_df.drop(['attributes'], axis=1, inplace=True)
buyer_df.rename(columns=buyer_lookup, inplace=True)

merged_df = pandas.merge(sidelot_df, buyer_df, on='address', how='inner')

merged_df['closing_date'] = merged_df['closing_date'].apply(lambda x: pandas.to_datetime(x))

import odo
odo.odo(merged_df, 'postgresql://{}@localhost/{}::dlba_sidelot'.format(env['PG_USER'], env['PG_DB']))
