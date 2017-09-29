#!/bin/bash
source /home/gisteam/.env

/home/gisteam/anaconda3/bin/python /home/gisteam/etl/process/dlba/00_download.py
# execute create table statements
# sudo psql -U gisteam -d etl < /home/gisteam/etl/process/dlba/01_create_views.sql

# do the things with the datasets
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py create_db_view --dir=dlba/side_lots
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py replace --dir=dlba/side_lots

/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py create_db_view --dir=dlba/own_it_now
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py replace --dir=dlba/own_it_now

/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py create_db_view --dir=dlba/auctions
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py replace --dir=dlba/auctions

/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py create_db_view --dir=dlba/for_sale
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py replace --dir=dlba/for_sale

