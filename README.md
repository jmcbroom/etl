# etl
ETL repository for DoIT work.

## Context

Each ETL has four steps, each with it's own .yml file:
- 00_metadata: Describes the process
- 01_extract: Gets the source data
- 02_transform: Cleans the data
- 03_load: Puts the data online

Depending on your data request, you might want to add a new view to an existing process (like DLBA or BSEED datasets for example), or create an entirely new process. If creating new, copy the four template .yml files in `example/` to a new folder under `process/`.

## Setup

Requires Python 3.5+ and preferably the Anaconda install.

`pip install -r requirements.txt` to install Python dependencies.

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

### 00_metadata.yml

```yml
name: <Title that describes this process, eg 'DLBA datasets'>
schema: <Postgres schema, eg 'dlba'>
notify: <'yes'|'no'>
```

### 01_extract.yml

An array of data sources to be extracted. 

Supported options:
- `database` from `database.py`: An Oracle or SQLServer database
- `salesforce` from `salesforce.py`: A Salesforce query
- `smartsheet` from `smartsheet.py`: A Smartsheet

Roadmap:
- `api` from `scf.py`: An endpoint to SeeClickFix Open 311
- `airtable`?
- `googlesheet`?

```yml
- database:
    type: <oracle|sql-server>
    prefix: <Origin database prefix, eg tm for tidemark>
    source: <Origin table name, eg CASEMAIN>
    destination: <Postgres table name, eg bseed.casemain>
    fields:
      - <Optional list of field names>
      - <If excluded, select *>

- salesforce:
    object: <SF object, eg Case>
    destination: <Postgres table name, eg dlba.account>
    fields:
      - <Optional list of field names>
      - <If excluded, select *>

- smartsheet: 
    id: <Smartsheet id>
    table: <Postgres table name, eg mmcc>
```

### 02_transform.yml

An array of steps to clean the data, which can include casting data types, geocoding addresses, scrubbing values, etc. These execute in order, think about them like steps in a recipe.

Supported options:
- `sql`: execute a list of custom SQL statements
- `geocode`: provide a table, address column, and geometry column

Roadmap:
- `anonymize`?
- `join`?

```yml
- type: sql
  statements:
    - <drop view if exists...>
    - <create view...>

- type: geocode
  table: <Postgres table or view, eg bseed.mmcc>
  add_col: <Existing address field>
  geom_col: <Geometry field name to be created>
```

### 03_load.yml

Currently supported options for `destination`:
- `Socrata`: A Socrata dataset
- `ArcGIS Online`: An ArcGIS Online feature layer

Roadmap:
- `Mapbox`: A Mapbox tileset

```yml
- to: Socrata
  id: <Socrata 4x4; if blank, create new dataset> 
  name: <Dataset title>
  table: <Postgres view to load the data from, eg bseed.annual_inspections_socrata>
  method: <replace|upsert>
  row_identifier: <Field name, optional>
  columns:
    <Human-readable name>:
      field: <Field name in postgres view>
      type: <Socrata data type>

- to: ArcGIS Online
  id: <AGO id; if blank, create new layer>
  file: <Filename>
  table: <Postgres view to load the data from, eg fire.angels_night_ago>
```
