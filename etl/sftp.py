import pysftp, pandas, odo
from os import environ as env
from datetime import datetime

def __init__(self):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    
    with pysft.Connection(env['SFTP_HOST'], port=2222, username=env['SFTP_USER'], password=env['SFTP_PASS'], cnopts=cnopts) as sftp:
        print('connected to server')

def get_file(self):
    pass

def write_file(self):
    pass

def to_postgres(self):
    pass
