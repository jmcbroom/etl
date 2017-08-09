echo "Beginning RMS (Private View) update..."
python $FILEROOT/01_get_update.py
echo "Anonymizing new locations..."
psql -d $PG_DB < $FILEROOT/02_anonymize_updates.sql
echo "Inserting new data into main table..."
psql -d $PG_DB -c "insert into rms_daily (select * from rms_daily_update)"
echo "Dropping temporary table..."
psql -d $PG_DB -c "drop table if exists rms_daily_update"
