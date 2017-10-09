!#/bin/bash
psql -d $PG_DB -c 'drop table dlba_asbestos_abatement cascade'
/home/gisteam/anaconda3/bin/python $FILEROOT/00_download.py