#!/bin/bash
source ~/.env
echo "Updating dataset..."
/home/gisteam/anaconda3/bin/python ~/newetl/tools/socrata.py update --dir=dpd/rms
echo "Replacing dataset..."
/home/gisteam/anaconda3/bin/python ~/newetl/tools/socrata.py replace --dir=dpd/rms
