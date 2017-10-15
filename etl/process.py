import os, yaml
from .utils import connect_to_pg, add_geom_column, geocode_addresses, exec_psql_query

DATA_DIR = '/home/jimmy/Work/etl/process'
connection = connect_to_pg()

class Process(object):
  def __init__(self, directory="project_greenlight"):
    self.basedir = "{}/{}".format(DATA_DIR, directory)
    with open("{}/00_metadata.yml".format(self.basedir), 'r') as f:
      m = yaml.load(f)
    self.schema = m['schema']
    self.name = m['name']
  
  def extract(self):
    from etl import Smartsheet as smartsheet
    with open("{}/01_extract.yml".format(self.basedir), 'r') as f:
      self.e = yaml.load(f)
    for src, config in self.e.items():
      if src == 'smartsheet':
        s = smartsheet(config['id'])
        s.to_postgres(self.schema, config['table'])
      # stub these out for future work
      elif src == 'database':
        pass
      elif src == 'salesforce':
        pass
      else:
        print("I don't know this source type: {}".format(src))

  def transform(self):
    with open("{}/02_transform.yml".format(self.basedir), 'r') as f:
      self.t = yaml.load(f)

    # if there's a geocode transform...
    if self.t['geocode']:
      opts = self.t['geocode']
      # add geometry column and index, specified in YML
      add_geom_column(connection, opts['table'], opts['geom_col'], self.schema)
      # loop through addresses, send to direccion.Address & populate geom column
      geocode_addresses(connection, "{}.{}".format(self.schema, opts['table']), opts['add_col'], opts['geom_col'])

    # execute array of custom sql
    for statement in self.t['sql']:
      exec_psql_query(connection, statement, verbose=True)

  def load(self):
    from etl import Socrata
    with open("{}/03_load.yml".format(self.basedir), 'r') as f:
      self.l = yaml.load(f)
      print(self.l)
    for dest, cfg in self.l.items():
      if dest == 'socrata':
        cfg['name'] = self.name
        s = Socrata(cfg)
        if not cfg['id']:
          cfg['id'] = s.create_dataset()
          with open('{}/03_load.yml'.format(self.basedir), 'w') as g:
            self.l['socrata']['id'] = cfg['id']
            yaml.dump(self.l, g, default_flow_style=False)
        s.replace()
      elif dest == 'arcgis-online':
        pass
      elif dest == 'mapbox':
        pass
      else:
        print("I don't know this destination type: {}".format(dest))