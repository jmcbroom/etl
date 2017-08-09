# etl
ETL repository for DoIT work.

## Setup

Requires Python 3.5+ and preferably the Anaconda install.

`pip -r requirements.txt` to install Python dependencies.

Copy `sample.env` to `.env` and add your secrets.

## Process

Workflows for getting data from various departments, cleaning it, and writing it to a central database. From there, datasets can be published to our Socrata open data portal, ESRI products, or other places.

Generally involves:
- Python or SQL scripts to access and download the source data, process it, and write it to a database table (usually Postgres). We name scripts in the order they run, eg use prefix `00_` or `01_` and so on
- Config file to define metadata and map fields from a db view to Socrata, or someplace else
- Shell scripts to schedule automatic updates via `cron`

## Tools

Tools for connecting to and working with various platforms and softwares. We use [python-fire](https://github.com/google/python-fire) to expose these classes to the command line.

More explanations soon.

### Socrata

Create a new dataset from the command line:
1. Run your scripts (assuming you've written scripts as described above in Process, and have at least `00_download.py` and `config.yml`):
```
cd etl/process/your/path
python 00_your_script.py
psql -d < 01_your_other_script.sql
```
2. Now, your data should be available in your database as a table. The name of this table should match what you set in `config.yml`
3. In the same terminal or a different tab, change directories from process to tools: `cd etl/tools`
4. Open Python in interactive mode: `python`
5. Create a new Socrata dataset using the following steps:
```
import socrata
ds=socrata.Dataset('your/path')
ds.create_db_view()
ds.create_dataset()
ds.socrata_id
```
6. Add the dataset ID echoed in the last command to your `config.yml`. At this point you should have a new dataset with an ID and columns, but 0 rows
7. Now, still in interactive mode, fill your dataset with actual rows of data: `ds.replace()`
8. That's it! You should see a success message in Slack's `#z_etl` channel and a data-full dataset on Socrata

Update this dataset in the future using a `cron` job and the `ds.full_replace()` or `ds.upsert()` commands.
