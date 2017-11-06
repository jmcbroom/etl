import os, yaml
from .utils import connect_to_pg, add_geom_column, exec_psql_query, drop_table_if_exists

from etl.slack import SlackMessage

DATA_DIR = './process'
connection = connect_to_pg()

class Process(object):
  def __init__(self, directory="project_greenlight"):
    self.basedir = "{}/{}".format(DATA_DIR, directory)
    self.refresh()
    self.process_name = self.m['name']
    self.schema = self.m['schema']
    self.notify = True
  
  def refresh(self):
    with open("{}/00_metadata.yml".format(self.basedir), 'r') as f:
      self.m = yaml.load(f)
    with open("{}/01_extract.yml".format(self.basedir), 'r') as f:
      self.e = yaml.load(f)
    with open("{}/02_transform.yml".format(self.basedir), 'r') as f:
      self.t = yaml.load(f)
    with open("{}/03_load.yml".format(self.basedir), 'r') as f:
      self.l = yaml.load(f)
  
  def extract(self):
    for source in self.e:
      for srctype, params in source.items():

        if srctype == 'smartsheet':
          from .smartsheet import Smartsheet
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
          from .salesforce import SfTable
          params['schema'] = self.schema
          drop_table_if_exists(connection, params['destination'])
          q = SfTable(params)
          q.to_postgres()

        else:
          print("I don't know this source type: {}".format(srctype))

  def transform(self):
    for s in self.t:
      if s['type'] == 'geocode':
        from etl.geocode import GeocodeTable
        # add geometry column and index, specified in YML
        add_geom_column(connection, s['table'], s['geom_col'])
        # batch geocode addresses
        GeocodeTable(s['table'], s['add_col'], s['geom_col']).geocode_rows()

      if s['type'] == 'sql':
        for statement in s['statements']:
          exec_psql_query(connection, statement, verbose=True)

  def load(self):
    self.destinations = [ d['to'] for d in self.l ]
    for d in self.l:
      destination = d.pop('to', None)
      if destination == 'Socrata':
        from .socrata import Socrata
        s = Socrata(d)
        s.update()
        self.msg.react('dart')
        # self.msg.comment('Updated Socrata')

      elif destination == 'ArcGIS Online':
        from .arcgis import AgoLayer
        l = AgoLayer(d)
        l.publish()
        self.msg.react('globe_with_meridians')
        # self.msg.comment('Updated ArcGIS Online')

      elif destination == 'Mapbox':
        pass

      else:
        print("I don't know this destination type: {}".format(destination))

  def update(self):
    if self.notify:
      self.msg = SlackMessage({"text": "Starting update: {}".format(self.process_name)})
      self.msg.send()
    self.extract()
    self.transform()
    self.load()