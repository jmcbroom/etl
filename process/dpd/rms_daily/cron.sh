#!/bin/bash
source ~/.env
echo "Updating RMS (Private View) dataset..."
/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py update --dir=dpd/rms_daily
echo "Replacing dataset..."

/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py create_db_view --dir=dpd/rms_daily
/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py replace --dir=dpd/rms_daily

/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py create_db_view --dir=dpd/rms_public
/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py replace --dir=dpd/rms_public

