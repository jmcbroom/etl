#!/bin/bash
psql -d $PG_DB -c "drop table if exists ocp cascade;"
/home/gisteam/anaconda3/bin/python ${FILEROOT}/01_download.py
