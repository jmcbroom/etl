#!/bin/bash
#!/path/to/anaconda3/bin/python

source /path/to/etl/.env
source /path/to/.bashrc

echo updating contracts

psql -d etl -c 'drop table if exists ocp cascade;'
/path/to/anaconda3/bin/python /path/to/etl/sftp/01_download.py
/path/to/anaconda3/bin/python /path/to/etl/sftp/socrata_replace.py

echo finished replacing rows in Socrata
