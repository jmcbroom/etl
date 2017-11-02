import os, yaml
from .utils import connect_to_pg, add_geom_column, exec_psql_query, drop_table_if_exists

from etl.slack import SlackMessage

DATA_DIR = './process'
connection = connect_to_pg()

class Process(object):
  def __init__(self, directory="project_greenlight"):
    self.basedir = "{}/{}".format(DATA_DIR, directory)
    with open("{}/00_metadata.yml".format(self.basedir), 'r') as f:
      m = yaml.load(f)
    self.schema = m['schema']
    self.name = m['name']
    print(m)
    if m['notify'] == 'yes':
      self.notify = True
    else:
      self.notify = False
  
  def extract(self):
    from .smartsheet import Smartsheet
    from .salesforce import SfTable
    with open("{}/01_extract.yml".format(self.basedir), 'r') as f:
      self.e = yaml.load(f)
    for source in self.e:
      for srctype, params in source.items():
        if srctype == 'smartsheet':
          s = Smartsheet(params['id'])
          drop_table_if_exists(connection, "{}.{}".format(self.schema, params['table']))
          s.to_postgres(self.schema, params['table'])
        elif srctype == 'database':
          from .database import DbTable
          if not params['columns']:
            t = DbTable(params['type'], params['source'], '*', params['destination'], params['prefix'])
          else:
            t = DbTable(params['type'], params['source'], params['columns'], params['destination'], params['prefix'])
          drop_table_if_exists(connection, params['destination'])
          t.to_postgres()
        elif srctype == 'salesforce':
          params['schema'] = self.schema
          q = SfTable(params)
          q.to_postgres()
        else:
          print("I don't know this source type: {}".format(srctype))

  def transform(self):
    with open("{}/02_transform.yml".format(self.basedir), 'r') as f:
      self.t = yaml.load(f)
    for step in self.t:
      for k, v in step.items():
        if k == 'geocode':
          from etl.geocode import GeocodeTable
          # add geometry column and index, specified in YML
          add_geom_column(connection, v['table'], v['geom_col'])
          # loop through addresses, send to direccion.Address & populate geom column
          GeocodeTable(v['table'], v['add_col'], v['geom_col']).geocode_rows()
        if k == 'sql':
          for statement in v:
            exec_psql_query(connection, statement, verbose=True)

  def load(self):
    from .socrata import Socrata
    from .arcgis import AgoLayer
    self.destinations = []
    with open("{}/03_load.yml".format(self.basedir), 'r') as f:
      self.l = yaml.load(f)
    for dest, cfg in self.l.items():
      self.destinations.append(dest)
      if dest == 'socrata':
        cfg['name'] = self.name
        s = Socrata(cfg)
        if not cfg['id']:
          cfg['id'] = s.create_dataset()
          with open('{}/03_load.yml'.format(self.basedir), 'w') as g:
            self.l['socrata']['id'] = cfg['id']
            yaml.dump(self.l, g, default_flow_style=False)
        s.update()
      elif dest == 'arcgis-online':
        l = AgoLayer(cfg)
        l.publish()
      elif dest == 'mapbox':
        pass
      else:
        print("I don't know this destination type: {}".format(dest))

  def update(self):
    if self.notify:
      msg = SlackMessage({"text": "Starting update: {}".format(self.proc.name)})
      msg.send()
    self.extract()
    self.transform()
    self.load()
    if self.notify:
      for d in self.destinations:
        if d == 'arcgis-online':
          msg.react('briefcase')
        elif d == 'socrata':
          msg.react('umbrella')