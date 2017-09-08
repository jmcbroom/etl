#!/usr/bin/env python
import sqlalchemy
import cx_Oracle
import pandas
import odo
from os import environ as env

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


# connection to tidemark
engine = sqlalchemy.create_engine('oracle://{}:{}@{}:{}/{}'.format(env['TM_USER'], env['TM_PASS'], env['TM_HOST'], env['TM_PORT'], env['TM_DB']))
tm_conn = engine.connect()
print('Connected to Tidemark', tm_conn)

# local connection
local_pg_engine = sqlalchemy.create_engine('postgresql+psycopg2://gisteam@localhost/etl')
pg_conn = local_pg_engine.connect()
print(pg_conn)

lookup = {
    'BG_ELV_DET_F': 'elevator',
    'BG_PLM_DET_F': 'plumbing',
    'BG_ELE_DET_F': 'electrical',
    'BG_BPV_DET_F': 'boiler',
    'BG_MEC_DET_F': 'mechanical'
}

import pandas as pd, odo
for k, v in lookup.items():
    print("Getting {} from Tidemark..".format(k))
    df = pd.read_sql("select * from {}".format(k), tm_conn)
    df['parcel_no'] = df['parcel_no'].apply(lambda x: clean_pnum(x))
    pg_conn.execute("drop table if exists bseed_{}".format(v))
    print("Inserting {} to local PG as {}".format(k, v))
    odo.odo(df, 'postgresql://gisteam@localhost/etl::bseed_{}'.format(v))
