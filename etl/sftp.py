import pysftp, pandas, odo, os
from os import environ as env
from datetime import datetime
from .utils import df_to_pg

def clean_cols(name):
  name = name.replace(":", "")
  name = name.replace(" ", "_").lower().strip()
  return name

def rename_cols(df):
  return df.rename(columns=lambda x: clean_cols(x), inplace=True)

class Sftp(object):
  """Connect to an SFTP server and retrieve a file."""

  def __init__(self, host='moveit'):
    self.host = host

    if self.host == 'novatus':
      cnopts = pysftp.CnOpts()
      cnopts.hostkeys = None
      with pysftp.Connection(env['NOVATUS_HOST'], port=2222, username=env['NOVATUS_USER'], password=env['NOVATUS_PASS'], cnopts=cnopts) as sftp:
        print('connected to {}'.format(env['NOVATUS_HOST']))
      
        today = datetime.today().strftime('%m%d%Y')
        file_name = 'Contracts_{}.csv'.format(today)
        path = '/outgoing/' + file_name

        if sftp.isfile(path):
          # copy file from sftp to local dir, read as df, then delete local copy
          sftp.get(path, preserve_mtime=True)
          self.df = pandas.read_csv(file_name)
          rename_cols(self.df)
          os.remove(file_name)
          print('got file, read into df')
        else:
          print('No file named {} found'.format(file_name))
          pass

    elif self.host == 'moveit':
      cnopts = pysftp.CnOpts()
      cnopts.hostkeys = None
      with pysftp.Connection(env['MOVEIT_HOST'], port=22, username=env['MOVEIT_USER'], password=env['MOVEIT_PASS'], cnopts=cnopts) as sftp:
        print('connected to {}'.format(env['MOVEIT_HOST']))

        file_name = 'purchase_agreements_open_data_test.csv'
        path = '/Home/IET/PO/' + file_name
          
        if sftp.isfile(path):
          sftp.get(path, preserve_mtime=True)
          self.df = pandas.read_csv(file_name)
          os.remove(file_name)
          print('got file, read into df')
        else:
          print('No file named {} found.'.format(file_name))
          pass
    
  def to_postgres(self):
    df_to_pg(self.df, 'ocp', 'purchase_agreements')
    print('sent to pg')
