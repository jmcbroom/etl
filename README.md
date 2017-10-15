# etl
ETL repository for DoIT work.

## Setup

Requires Python 3.5+ and preferably the Anaconda install.

`pip -r requirements.txt` to install Python dependencies.

Copy `sample.env` to `.env` and add your secrets.

## Usage

```python
import etl
# load YMLs for a process
gl = etl.Process('project_greenlight')
# extract from sources
gl.extract()
# transform (operations & sql)
gl.transform()
# load to destinations
gl.load()
```

### 01_extract.yml

```yml
source:
  options...
source_two:
  options...
```

Currently supported options for `source`:
- `smartsheet`: A Smartsheet

Roadmap:
- `database`: Database table(s) from: SQL Server, Oracle...
- `salesforce`: A Salesforce query
- `googlesheet`: A Google Sheet
- `airtable`: Table(s) from an Airtable base

### 02_transform.yml

```yml
geocode:
  table: <the table with the addresses>

sql:
  - 'update...'
  - 'create view...'
  - 'insert...'
```

Right now, you can only geocode and do SQL.

### 03_load.yml

```
destination:
  options...
destination_two:
  options...
```

Currently supported options for `destination`:
- `socrata`: A Socrata dataset

Roadmap:
- `arcgis-online`: An ArcGIS Online feature layer
- `mapbox`: A Mapbox tileset
