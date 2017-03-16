# etl
ETL repository for DoIT work.

Using [python-fire](https://github.com/google/python-fire) to expose these classes to the command line.

## Setup

Requires Python 3.5+ and preferably the Anaconda install.

`pip -r requirements.txt` to install Python dependencies.

### `.env` / environment variables

```bash
# Smartsheet
export SMARTSHEET_TOKEN=

# Socrata
export SODA_TOKEN=
export SODA_PASS=
export SODA_USER=

# ArcGIS Online
export AGO_USER=
export AGO_PASS=

# SeeClickFix Organization Endpoint
export SCF_USER=
export SCF_PASS=
```

## tools
- smartsheets
- esri

## workflows
- seeclickfix aka improve detroit
