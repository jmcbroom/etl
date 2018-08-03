import pysftp, pandas, odo, os
from os import environ as env
from datetime import datetime
from .utils import df_to_pg

def clean_cols(name):
  name = name.replace(":", "")
  name = name.replace("#", "")
  name = name.replace(" ", "_").lower().strip()
  return name

def rename_cols(df):
  return df.rename(columns=lambda x: clean_cols(x), inplace=True)

class Sftp(object):
  """Connect to an SFTP server and retrieve a file."""

  def __init__(self, host='moveit', destination='ocp.contracts', file='/tmp/sample.csv'):
    self.host = host
    self.destination = destination
    self.file = file

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

        file_name = 'purchase_agreements_open_data.csv'
        path = '/Home/IET/PO/' + file_name
          
        if sftp.isfile(path):
          sftp.get(path, preserve_mtime=True)
          self.df = pandas.read_csv(file_name)
          rename_cols(self.df)
          os.remove(file_name)
          print('got file, read into df')
        else:
          print('No file named {} found.'.format(file_name))
          pass
  
    elif self.host == 'pnc':
      cnopts = pysftp.CnOpts()
      cnopts.hostkeys = None
      with pysftp.Connection(env['MOVEITPROD_HOST'], port=22, username=env['MOVEITPROD_USER'], password=env['MOVEITPROD_PASS'], cnopts=cnopts) as sftp:
        print('connected to {}'.format(env['MOVEITPROD_HOST']))

        import arrow
        date = arrow.utcnow().shift(days=-1)
        file_name = "TrialBalance{}.csv".format(date.format('MMDDYYYY'))
        path = '/Home/IET/PNC/' + file_name
          
        if sftp.isfile(path):
          sftp.get(path, preserve_mtime=True)
          self.df = pandas.read_csv(file_name)
          rename_cols(self.df)
          os.remove(file_name)
          print('got file, read into df')
        else:
          print('No file named {} found.'.format(file_name))
          pass

    elif self.host == 'crimescape':
      with pysftp.Connection(env['CRIMESCAPE_HOST'], username=env['CRIMESCAPE_USER'], private_key=env['CRIMESCAPE_KEY']) as sftp:
        print('connected to {}'.format(env['CRIMESCAPE_HOST']))

        sftp.put(self.file, preserve_mtime=True)
        print('put {} on sftp'.format(self.file))

    else:
      pass
    
  def to_postgres(self):
    schema, table = self.destination.split('.')
    df_to_pg(self.df, schema, table)
    print('sent to postgres {}.{}'.format(schema, table))
