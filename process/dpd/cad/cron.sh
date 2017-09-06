#!/bin/bash
source ~/.env
$ANA_PYTHON ~/etl/tools/socrata.py update --dir=dpd/cad
$ANA_PYTHON ~/etl/tools/socrata.py upsert --dir=dpd/cad
