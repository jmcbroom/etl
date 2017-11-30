import pysftp, pandas, odo, os
from os import environ as env
from datetime import datetime
from .utils import df_to_pg

class Sftp(object):
  """Connect to an SFTP server and retrieve a file."""

  def __init__(self):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(env['SFTP_HOST'], port=2222, username=env['SFTP_USER'], password=env['SFTP_PASS'], cnopts=cnopts) as sftp:
      print('connected to server')
      
      today = datetime.today().strftime('%m%d%Y')
      file_name = 'Contracts_{}.csv'.format(today)
      path = '/outgoing/' + file_name

      if sftp.isfile(path):
        # copy file from sftp to local dir, read as df
        sftp.get(path, preserve_mtime=True)
        self.df = pandas.read_csv(file_name)
        os.remove(file_name)
        print('got file, read into df')
      else:
        print('No file named {} found'.format(file_name))
        pass
    
  def to_postgres(self):
    df_to_pg(self.df, 'ocp', 'contracts')
    print('sent to pg')
