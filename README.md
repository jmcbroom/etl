# etl
ETL repository for DoIT work.

## Setup

Requires Python 3.5+ and preferably the Anaconda install.

`pip -r requirements.txt` to install Python dependencies.

Copy `sample.env` to `.env` and add your secrets.

## Usage

```python
import etl
gl = etl.Process('project_greenlight')
gl.extract()
gl.transform()
gl.load()
```
