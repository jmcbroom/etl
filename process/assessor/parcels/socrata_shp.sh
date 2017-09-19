#!/bin/bash
#ogr2ogr -f "ESRI Shapefile" parcels -nln parcels -s_srs epsg:2898 -t_srs epsg:4326 -skipfailures pg:dbname=etl base.joined
pgsql2shp -f parcels etl base.joined
