!#/bin/bash
psql -d $PG_DB -c 'drop table dlba_upcoming_demos cascade'
/home/gisteam/anaconda3/bin/python $FILEROOT/00_download.py
