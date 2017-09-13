#!/bin/bash
source /home/gisteam/.env
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py update --dir="assessor/parcels"
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py create_db_view --dir="assessor/parcels"
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py delete_all_rows --dir="assessor/parcels"
/home/gisteam/anaconda3/bin/python /home/gisteam/etl/tools/socrata.py upsert --dir="assessor/parcels"
