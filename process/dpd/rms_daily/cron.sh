#!/bin/bash
source ~/.env
echo "Updating RMS (Private View) dataset..."
/home/gisteam/anaconda3/bin/python ~/newetl/tools/socrata.py update --dir=dpd/rms_daily
echo "Replacing dataset..."
/home/gisteam/anaconda3/bin/python ~/newetl/tools/socrata.py replace --dir=dpd/rms_daily
