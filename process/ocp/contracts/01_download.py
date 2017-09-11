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
    print('No file found on sftp today')
    pass

# create a dataframe from the local copy
df = pandas.read_csv(file_path)

def clean_cols(name):
	"""Format column names"""
	name = name.replace(":", "")
	name = name.replace(" ", "_").lower().strip()
	return name

# rename cols
df.rename(columns=lambda x: clean_cols(x), inplace=True)

def make_float(value):
	"""Format money strings as number"""
	value = value.replace("$", "")
	return float(value)

# format money col
df['contract_value'] = df['contract_value'].apply(lambda x: make_float(x))

def clean_company(value):
  """Capitalize name, remove commas and end punctuation"""
  value = value.replace(",", "")
  if value.endswith("."):
    value = value[:-1]
  else:
    value = value
  return value.upper().strip()

# format company name col
df['company_company_name'] = df['company_company_name'].apply(lambda x: clean_company(x))

# uppercase other cols for consistency
df['company_city'] = df['company_city'].str.upper()
df['company_state'] = df['company_state'].str.upper()
df['contract_contract_purpose'] = df['contract_contract_purpose'].str.upper()

def abbreviate_state(value):
  """Abbreviate Michigan to MI"""
  if value == "MICHIGAN":
    value = "MI"
  else:
    pass
  return value

# format state col
df['company_state'] = df['company_state'].apply(lambda x: abbreviate_state(x))

# replace some NaNs with column-specific text
df['contract_document'] = df['contract_document'].fillna("No document")
df['contract_nigp_code'] = df['contract_nigp_code'].fillna("No code")

# replace all other NaNs with empty strings
df = df.fillna('')

# sort dataframe alphabetically by codes, then by amount largest to smallest
df = df.sort_values(by=['contract_nigp_code','contract_value'], ascending=[True,False])

# write the dataframe to a central db
odo.odo(df, 'postgresql://{}@localhost/{}::ocp'.format(os.environ['PG_USER'], os.environ['PG_DB']))
