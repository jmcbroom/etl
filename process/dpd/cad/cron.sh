#!/bin/bash
source ~/.env
$ANA_PYTHON ~/newetl/tools/socrata.py update --dir=dpd/cad
$ANA_PYTHON ~/newetl/tools/socrata.py upsert --dir=dpd/cad
