import os, pysftp, pandas, odo
from datetime import datetime

# get todays date and format as MMDDYYYY string
today = datetime.today().strftime('%m%d%Y')

# set paths to todays data
file_path = 'Contracts_{}.csv'.format(today)
remote_path = '/outgoing/' + file_path

# connect to the sftp server and get a copy of the file if it exists
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(os.environ['SFTP_HOST'], port=2222, username=os.environ['SFTP_USER'], password=os.environ['SFTP_PASS'], cnopts=cnopts) as sftp:
    print('Connected to server')

    if sftp.isfile(remote_path): # returns bool
        sftp.get(remote_path, preserve_mtime=True)
        print('Copied remote file to local dir')

    else:
        print('No file found at {}'.format(remote_path))

# create a dataframe from the local copy
df = pandas.read_csv(file_path)

# write the dataframe to a central db
odo.odo(df, 'postgresql:://gisteam@localhost/etl::ocp')
