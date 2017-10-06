#!/usr/bin/env python
import simple_salesforce
import pandas
import odo
from os import environ as env

SF = simple_salesforce.Salesforce(env['SF_USER'], env['SF_PASS'], env['SF_TOKEN'])
print('Connected to SF ', SF)

# lookup fields from the dba comm demo object and related property account
lookup = {
    'Address__c': 'address',
    'BSEED_Final_Grade_Approved__c': 'bseed_final_grade_approved',
    'DEMO_Final_Grade_Approved_Date__c': 'demo_final_grade_approved',
    'Parcel_ID__c': 'parcel_id',
    'DEMO_Batch_Contractor_Name_del1__r.Name': 'demo_batch_contractor_name_del1_name',
    'Abatement_Sub_Contractor__c': 'abatement_sub_contractor',
    'DEMO_ASB_Abatement_Contractor__r.Name': 'demo_asb_abatement_contractor_name',
    'DEMO_ASB_Survey_Contractor__r.Name': 'demo_asb_survey_contractor_name',
    'ASB_Inspectors_Name__c': 'asb_inspectors_name',
    'ASB_Abatement_Verification_Contractor__r.Name': 'asb_abatement_verification_contractor_name',
    'ASB_Verifier_Name__c': 'asb_verifier_name',
    'Demo_Contractor_Proceed_Date__c': 'demo_contractor_proceed_date',
    'ASB_Abatement_Start_Date__c': 'asb_abatement_start_date',
    'DEMO_ASB_Abatement_Date__c': 'demo_asb_abatement_date',
    'ASB_Post_Abatement_Insp_Date__c': 'asb_post_abatement_insp_date',
    'DEMO_ASB_Post_Abatement_Approval_Date__c': 'demo_asb_post_abatement_approval_date',
    'Demo_ASB_Post_Abatement_Failed_Date__c': 'demo_asb_post_abatement_failed_date',
    'ASB_Post_Abatement_Notes__c': 'asb_post_abatement_notes',
    'ASB_Post_Abatement_Times_Failed__c': 'asb_post_abatement_times_failed',
    'ASB_Post_Abatement_Verification_Status__c': 'asb_post_abatement_verification_status',
    'DEMO_Planned_Knock_Down_Date__c': 'demo_planned_knock_down_date',
    'DEMO_Knock_Down_Date__c': 'demo_knock_down_date',
    'Socrata_Projected_Knocked_By_Date__c': 'socrata_projected_knocked_by_date',
    'ASB_Document_URL__c': 'asb_document_url',
    'ASB_Post_Abatement_Document_URL__c': 'asb_post_abatement_document_url',
    'Property__r.Longitude__c': 'longitude',
    'Property__r.Latitude__c': 'latitude',
    'Property__r.ZIP_Code__c': 'zip_code'
}

# query salesforce
query = """
Select {} from Case
""".format(",".join(lookup.keys()))

res = SF.query_all(query)

# make a dataframe with our result
df = pandas.DataFrame.from_records(res['records'])
df.rename(columns=lookup, inplace=True)
df.drop('attributes', inplace=True, axis=1)

print('Created dataframe ', df.shape)