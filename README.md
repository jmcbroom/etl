# etl
ETL repository for DoIT work.

## Setup

Requires Python 3.5+ and preferably the Anaconda install.

`pip -r requirements.txt` to install Python dependencies.

Copy `sample.env` to `.env` and add your secrets.

## Process

Workflows for getting data from various departments, cleaning it, and writing it to a central database. From there, datasets can be published to our Socrata open data portal, ESRI products, or other places.

Generally involves:
- Python or SQL scripts to access the source data, process it, and write it to a db table (usually Postgres)
- Config file to define metadata and map fields from a db view to Socrata, or someplace else
- Shell scripts to schedule automatic updates via `cron`

## Tools

Tools for connecting to and working with various platforms and softwares. We use [python-fire](https://github.com/google/python-fire) to expose these classes to the command line.

More explanations soon.
