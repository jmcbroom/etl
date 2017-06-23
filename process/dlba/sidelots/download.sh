psql -d $PG_DB -c "drop table if exists dlba_sidelot cascade;"
python $FILEROOT/00_sidelots.py
