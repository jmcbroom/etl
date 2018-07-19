import os, yaml
from .utils import connect_to_pg, add_geom_column, exec_psql_query, drop_table_if_exists, create_psql_view, create_psql_table

DATA_DIR = "{}/process".format(os.environ['ETL_ROOT'])
connection = connect_to_pg()

class Dataset(object):
  def __init__(self, name, basedir, schema):
    self.name = name
    self.basedir = basedir
    self.schema = schema
    self.refresh()

  def refresh(self):
    with open("{}/metadata/{}.yml".format(self.basedir, self.name), 'r') as f:
      self.m = yaml.load(f)
    with open("{}/transform/{}.yml".format(self.basedir, self.name), 'r') as f:
      self.t = yaml.load(f)
    with open("{}/load/{}.yml".format(self.basedir, self.name), 'r') as f:
      self.l = yaml.load(f)

  def transform(self):
    for s in self.t:
      if s['type'] == 'geocode':
        from etl.geocode import GeocodeTable
        # add geometry column and index, specified in YML
        add_geom_column(connection, s['table'], s['geom_col'])
        # batch geocode addresses
        if 'parcel_col' in s.keys():
          GeocodeTable(s['table'], s['add_col'], s['geom_col'], s['parcel_col']).geocode_rows()
        elif 'where_clause' in s.keys() and 'parcel_col' in s.keys():
          GeocodeTable(s['table'], s['add_col'], s['geom_col'], s['parcel_col'], s['where_clause']).geocode_rows()
        elif 'where_clause' in s.keys() and 'parcel_col' not in s.keys():
          GeocodeTable(s['table'], s['add_col'], s['geom_col'], None, s['where_clause']).geocode_rows()
        else:
          GeocodeTable(s['table'], s['add_col'], s['geom_col']).geocode_rows()

      if s['type'] == 'sql':
        for statement in s['statements']:
          exec_psql_query(connection, statement, verbose=True)

      if s['type'] == 'create_view':
        create_psql_view(connection, "{}.{}".format(self.schema, s['view_name']), s['as'])

      if s['type'] == 'create_table':
        create_psql_table(connection, "{}.{}".format(self.schema, s['table_name']), s['as'])

      if s['type'] == 'anonymize_text_location':
        from etl.anonymize import AnonTextLocation
        atl = AnonTextLocation(s['table'], s['column'], s['set_flag'])
        atl.anonymize()

      if s['type'] == 'anonymize_geometry':
        from etl.anonymize import AnonGeometry
        ag = AnonGeometry(s['table'], s['against'])
        ag.anonymize()

      if s['type'] == 'lookup':
        print(s)
        from etl.lookup import LookupValues
        look = LookupValues(s['table'], s['lookup_field'], s['file'], s['match_field'], s['method'], s['set_flag'])
        look.lookup()
  
  def load(self):
    for d in self.l:

      # switch on destination
      destination = d.pop('to', None)

      if destination == 'Socrata':
        from .socrata import Socrata
        s = Socrata(d)
        s.update()

      elif destination == 'ArcGIS Online':
        from .arcgis import AgoLayer
        l = AgoLayer(d)
        l.publish()

      elif destination == 'SFTP':
        from .sftp import Sftp
        s = Sftp(d['host'], None, d['file'])

      elif destination == 'Mapbox':
        from .mapbox import MapboxUpload
        m = MapboxUpload(d)
        m.upload()

      else:
        print("I don't know this destination type: {}".format(destination))

class Process(object):
  def __init__(self, directory="project_greenlight"):
    self.basedir = "{}/{}".format(DATA_DIR, directory)
    self.refresh()
    self.process_name = self.m['name']
    self.schema = self.m['schema']
    self.datasets = self.m['datasets']

  def refresh(self):
    with open("{}/00_metadata.yml".format(self.basedir), 'r') as f:
      self.m = yaml.load(f)
    with open("{}/01_extract.yml".format(self.basedir), 'r') as f:
      self.e = yaml.load(f)

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

          # assign params['where']
          if 'where' not in params.keys():
            params['where'] = "1=1"
          elif type(params['where']) is str:
            pass
          elif type(params['where']) is dict:
            w = params['where']
            value = connection.execute("select {} from {}".format(w['value'], w['table'])).fetchone()[0]
            clause = "{} {} {}".format(w['field'], w['condition'], value)
            params['where'] = clause

          if 'columns' not in params.keys():
            t = DbTable(params['type'], params['source'], '*', params['destination'], params['prefix'], params['where'])
          else:
            t = DbTable(params['type'], params['source'], params['columns'], params['destination'], params['prefix'], params['where'])

          drop_table_if_exists(connection, params['destination'])
          t.to_postgres()

        elif srctype == 'salesforce':
          from .salesforce import SfTable
          params['schema'] = self.schema
          drop_table_if_exists(connection, params['destination'])
          q = SfTable(params)
          q.to_postgres()

        elif srctype == 'api':
          if params['domain'] == 'seeclickfix':
            from .scf import Seeclickfix
            drop_table_if_exists(connection, params['destination'])
            s = Seeclickfix()
            s.to_postgres()
          else:
            pass

        elif srctype == 'sftp':
          from .sftp import Sftp
          drop_table_if_exists(connection, params['destination'])
          s = Sftp(params['host'], params['destination'])
          s.to_postgres()

        elif srctype == 'mapbox':
          from .mapbox import MapboxDataset
          m = MapboxDataset(params)
          drop_table_if_exists(connection, params['destination'])
          m.to_postgres()

        elif srctype == 'airtable':
          from .airtable import AirtableTable
          a = AirtableTable(params)
          drop_table_if_exists(connection, params['destination'])
          a.to_postgres()
          
        else:
          print("I don't know this source type: {}".format(srctype))

  def update(self, dataset=None):
    # Extract our data that the Datasets require
    self.extract()

    # Loop through our datasets
    for d in self.datasets:

      # Requires dataset name and basedir
      ds = Dataset(d, self.basedir, self.schema)

      # These two steps now belong to Dataset
      ds.transform()
      # ds.load()
