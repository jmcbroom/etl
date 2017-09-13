echo $FILEROOT
echo "Beginning parcels update..."
/home/gisteam/anaconda3/bin/python $FILEROOT/01_download.py
echo "Joining to attrib table"
psql -d $PG_DB < $FILEROOT/02_join.sql
