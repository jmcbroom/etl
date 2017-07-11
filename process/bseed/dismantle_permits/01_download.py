#!/usr/bin/env python
import sqlalchemy
import cx_Oracle
import pandas
import odo
from os import environ as env

engine = sqlalchemy.create_engine('oracle://{}:{}@{}:{}/{}'.format(env['TM_USER'], env['TM_PASS'], env['TM_HOST'], env['TM_PORT'], env['TM_DB']))
conn = engine.connect()
print('Connected to Tidemark', conn)

# get 3 types of inspections on and after 01/2014 from CASE_ACTION (note open hole type needs a trailing white space)
actions = conn.execute("select * from CASE_ACTION where CSA_DATE3 >= DATE '2014-01-01' and ACTION_DESCRIPTION in ('Final Grade Inspection', 'Open Hole Demo Inspection ', 'Winter Grade Inspection')")

# format the results as a dataframe
caseaction_df = pandas.DataFrame(actions.fetchall())
caseaction_df.columns = actions.keys()
print('Fetched {} actions'.format(caseaction_df.shape[0]))

# get everything from CASE_PARCEL
parcels = conn.execute("select * from CASE_PARCEL")

# format the results as a dataframe
caseparcel_df = pandas.DataFrame(parcels.fetchall())
caseparcel_df.columns = parcels.keys()
print('Fetched {} parcels'.format(caseparcel_df.shape[0]))

# join actions to parcels
action_parcels_df = caseaction_df.merge(caseparcel_df, on='csm_caseno', how='inner')
print('Joined actions and parcels for a total of {} rows and {} columns'.format(action_parcels_df.shape[0], action_parcels_df.shape[1]))

# clean Tidemark parcel nums to standard format
def clean_pnum(pnum):
    # first, change double spaces to a single white space
    pnum = pnum.replace('  ', ' ')
    
    # it's zeroes; return nothing
    if pnum in ['0', '00', '000']:
        return None
    
    # there's a dot with stuff after it
    if '.' in pnum and pnum[-1] != '.':
        end = pnum[pnum.find('.'):]
        beginning = pnum.split('.')[0]
    # there's a dot at the very end
    elif '.' in pnum and pnum[-1] == '.':
        end = pnum[-1]
        beginning = pnum[:-1]
    # there's a dash with stuff after it
    elif '-' in pnum and pnum[-1] != '-':
        end = pnum[pnum.find('-'):]
        beginning = pnum.split('-')[0]
    else:
        end = ''
        beginning = pnum
    
    # only a few of these cases that don't have a space
    if ' ' not in beginning and len(beginning) == 8:
        return beginning + end
    elif ' ' not in beginning and len(beginning) < 8:
        return beginning.zfill(8) + end
    
    # most will have a space
    if ' ' in beginning:
        ward = beginning.split(' ')[0]
        lot = beginning.split(' ')[1]
        if len(ward) == 1:
            ward = ward.zfill(2)
        return ward + lot.zfill(6) + end

# add a new col with clean parcels
action_parcels_df['clean_parcel_no'] = action_parcels_df['prc_parcel_no'].apply(lambda x: clean_pnum(x))

# add a new col with a unique id
action_parcels_df['unique_id'] = action_parcels_df['csa_id'] + "_" + action_parcels_df['prc_parcel_no']

# send the dataframe to postgres
odo.odo(action_parcels_df, 'postgresql://{}@localhost/{}::bseed_dismantle_permits'.format(env['PG_USER'], env['PG_DB']))
print('Wrote the dataframe to postgres table: bseed_dismantle_permits')
