echo "Beginning CAD update..."
python $FILEROOT/01_get_updates.py
echo "Anonymizing new locations..."
psql -d $PG_DB < $FILEROOT/02_anonymize_updates.sql
echo "Inserting new data into main table..."
psql -d $PG_DB -c "insert into cad (select * from cad_update)"
echo "Dropping temporary table..."
psql -d $PG_DB -c "drop table if exists cad_update"