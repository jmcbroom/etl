psql -d $PG_DB -c "drop table if exists ocp cascade;"
python ${FILEROOT}/01_download.py
