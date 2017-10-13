import os, yaml

DATA_DIR = '/home/jimmy/Work/etl/process'

import sqlalchemy
engine = sqlalchemy.create_engine('postgresql+psycopg2://{}/{}'.format(os.environ['PG_CONNSTR'], os.environ['PG_DB']))
connection = engine.connect()

class Process(object):
  def __init__(self, directory="project_greenlight"):
    self.basedir = "{}/{}".format(DATA_DIR, directory)
    with open("{}/00_metadata.yml".format(self.basedir), 'r') as f:
      self.metadata = yaml.load(f)
    self.schema = self.metadata['schema']
  
  def extract(self):
    from etl import Smartsheet as smartsheet
    with open("{}/01_extract.yml".format(self.basedir), 'r') as f:
      self.e = yaml.load(f)
    for src, config in self.e.items():
      if src == 'smartsheet':
        s = smartsheet(config['id'])
        s.to_postgres(self.schema, config['table'])

  def transform(self):
    with open("{}/02_transform.yml".format(self.basedir), 'r') as f:
      self.t = yaml.load(f)
    for statement in self.t:
      print(statement)
      connection.execute(statement)

  def load(self):
    from etl import Socrata
    with open("{}/03_load.yml".format(self.basedir), 'r') as f:
      self.l = yaml.load(f)
      print(self.l)
    for dest, cfg in self.l.items():
      if dest == 'socrata':
        cfg['name'] = self.metadata['name']
        s = Socrata(cfg)
        if not cfg['id']:
          cfg['id'] = s.create_dataset()
          with open('{}/03_load.yml'.format(self.basedir), 'w') as g:
            self.l['socrata']['id'] = cfg['id']
            yaml.dump(self.l, g, default_flow_style=False)
        s.replace()