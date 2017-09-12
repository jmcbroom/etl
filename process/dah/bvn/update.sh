echo "Beginning Blight Violations update..."
/home/gisteam/anaconda3/bin/python $FILEROOT/01_download.py
echo "Inserting new data into main table..."
psql -d $PG_DB < $FILEROOT/02_transform.sql


