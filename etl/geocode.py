from arcgis import gis, geocoding
from os import environ as env
from .utils import connect_to_pg
import sqlalchemy

ago = gis.GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])

geocoders = {
  'composite': geocoding.get_geocoders(ago)[0],
  'address': geocoding.get_geocoders(ago)[1],
  'centerline': geocoding.get_geocoders(ago)[2]
}

class GeocodeTable(object):
  def __init__(self, table, addr_col='address', geom_col='geom', parcel_col=None, where_clause="1=1", geocoder='composite'):
    self.table = table
    self.addr_col = addr_col
    self.geom_col = geom_col
    self.parcel_col = parcel_col
    self.where_clause = where_clause
    self.geocoder = geocoders[geocoder]
  
  def geocode_rows(self):
    conn = connect_to_pg()
    res = conn.execute("select distinct {} from {} where {}".format(self.addr_col, self.table, self.where_clause))
    self.rows = [ r[0] for r in res.fetchall() ]

    # iterate through batches of 1000
    for i in range(0, len(self.rows), 1000):
      rows_to_geocode = self.rows[i:i+1000]
      results = geocoding.batch_geocode(rows_to_geocode, out_sr=4326, geocoder=self.geocoder)
      result_dict = dict(zip(rows_to_geocode, results))
      for add, res in result_dict.items():
        if res['attributes']['User_fld'] != "" and self.parcel_col:
          query = "update {} set {} = ST_SetSRID(ST_MakePoint({}, {}), 4326), {} = '{}' where {} = '{}'".format(
            self.table, 
            self.geom_col,
            res['location']['x'],
            res['location']['y'],
            self.parcel_col,
            res['attributes']['User_fld'],
            self.addr_col,
            add.replace("'", "''")
          )
          conn.execute(query)
        elif res['location']['x'] != 'NaN':
          query = "update {} set {} = ST_SetSRID(ST_MakePoint({}, {}), 4326) where {} = '{}'".format(
            self.table, 
            self.geom_col,
            res['location']['x'],
            res['location']['y'],
            self.addr_col,
            add.replace("'", "''")
          )
          conn.execute(query)
        else:
          pass