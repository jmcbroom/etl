#!/bin/bash
source ~/.env
/home/gisteam/anaconda3/bin/python ~/etl/tools/socrata.py full_replace --dir=/bseed/dismantle_permits
