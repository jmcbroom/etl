psql -d $PG_DB -c 'drop table dlba_upcoming_demos cascade'
python $FILEROOT/00_download.py
