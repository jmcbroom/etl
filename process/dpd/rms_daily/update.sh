echo "Beginning RMS (Private View) update..."
/home/gisteam/anaconda3/bin/python $FILEROOT/01_get_update.py
echo "Anonymizing new locations..."
psql -d $PG_DB < $FILEROOT/02_anonymize_updates.sql
echo "Inserting new data into main table..."
psql -d $PG_DB -c "alter table rms rename to rms_old"
psql -d $PG_DB -c "alter table rms_update rename to rms"
psql -d $PG_DB -c "drop table rms_old cascade"


