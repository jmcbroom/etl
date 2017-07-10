#!/bin/bash
source ~/.env
/home/gisteam/anaconda3/bin/python ~/newetl/tools/socrata.py update --dir=dpd/cad
/home/gisteam/anaconda3/bin/python ~/newetl/tools/socrata.py upsert --dir=dpd/cad
