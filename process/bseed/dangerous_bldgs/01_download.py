#!/usr/bin/env python
import sqlalchemy
import cx_Oracle
import pandas
import numpy as np
import odo
from os import environ as env

# connection to tidemark
engine = sqlalchemy.create_engine('oracle://{}:{}@{}:{}/{}'.format(env['TM_USER'], env['TM_PASS'], env['TM_HOST'], env['TM_PORT'], env['TM_DB']))
conn = engine.connect()
print('Connected to Tidemark', conn)

# query CASEMAIN
main = conn.execute("select * from CASEMAIN where CSM_PROJNO='PRJ2011-00690'")
cm_df = pandas.DataFrame(main.fetchall())
cm_df.columns = main.keys()

# get everything from BG_INV_ANLY
bg = conn.execute("select * from BG_INV_ANLY")
bg_df = pandas.DataFrame(bg.fetchall())
bg_df.columns = bg.keys()

# outer join where CASEMAIN.csm_caseno = BG_INV_ANLY.dng_case_no
bg_df.rename(columns={'dng_case_no': 'csm_caseno', 'PERMIT_#': 'permit_number', 'CONTRACT_#': 'contract_number', 'PARCEL_#': 'tidemark_parcel_number'}, inplace=True)
dng_blds_df = pandas.merge(cm_df, bg_df, how="outer", on="csm_caseno")

print('Fetched and joined CASEMAIN and BG_INV_ANLY, cleaning data...')

# clean Tidemark parcel nums to standard format
def clean_pnum(pnum):
    # if our param is a float, make it a string
    pnum = str(pnum)
    
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
dng_blds_df['clean_parcel_no'] = dng_blds_df['prc_parcel_no'].apply(lambda x: clean_pnum(x))

# concat address segments
def concat(*args):
  strs = [str(arg) for arg in args if not pandas.isnull(arg)]
  return ' '.join(strs) if strs else np.nan

np_concat = np.vectorize(concat)

# add a new col with clean street address
dng_blds_df['clean_st_address'] = np_concat(dng_blds_df['st_no'], dng_blds_df['st_dir'], dng_blds_df['st_name'])

# add col demo and set to "Demolished" if final_grade_comp is not Null
dng_blds_df['demolished'] = np.where(pandas.notnull(dng_blds_df['final_grade_comp']), 'Demolished', '')

# send the dataframe to postgres
odo.odo(dng_blds_df, 'postgresql://{}@localhost/{}::bseed_dangerous_bldgs'.format(env['PG_USER'], env['PG_DB']))
print('Sent to postgres')
