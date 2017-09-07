#!/bin/bash
source ~/.env
echo "Updating Blight Violations dataset..."
/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py update --dir=dah/bvn
echo "Replacing dataset..."

/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py create_db_view --dir=dah/bvn
/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py upsert --dir=dah/bvn