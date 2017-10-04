# unzip the initial load
unzip PARCELS.gdb.zip
# insert the shapes
ogr2ogr -f PostgreSQL pg:dbname=etl -s_srs epsg:2898 -t_srs epsg:2898 -overwrite -nln assessor.shapes -nlt GEOMETRY PARCELS.gdb PARCELS_080117
# insert the base parcel data
ogr2ogr -f PostgreSQL pg:dbname=etl -nln assessor.baseattr -overwrite PARCELS.gdb PARCELDATA18c
# cleanup
rm -rf ./PARCELS.gdb/