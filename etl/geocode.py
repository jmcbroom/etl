from arcgis import gis, geocoding

from os import environ as env

from .utils import connect_to_pg

ago = gis.GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])
composite = geocoding.get_geocoders(ago)[0]
address_points = geocoding.get_geocoders(ago)[1]
street_centerline = geocoding.get_geocoders(ago)[2]

class GeocodeTable(object):
  def __init__(self, table, addr_col='address', geom_col='geom'):
    self.table = table
    self.addr_col = addr_col
    self.geom_col = geom_col
  
  def geocode_rows(self):
    conn = connect_to_pg()
    res = conn.execute("select {} from {}".format(self.addr_col, self.table))
    self.rows = [ r[0] for r in res.fetchall() ]

    # iterate through batches of 1000
    for i in range(0, len(self.rows), 1000):
      print(i, i+1000)
      rows_to_geocode = self.rows[i:i+1000]
      results = geocoding.batch_geocode(rows_to_geocode, out_sr=4326, geocoder=composite)
      result_dict = dict(zip(rows_to_geocode, results))
      for add, res in result_dict.items():
        if res['location']['x'] != 'NaN':
          query = "update {} set {} = ST_SetSRID(ST_MakePoint({}, {}), 4326) where {} = '{}'".format(
            self.table, 
            self.geom_col,
            res['location']['x'],
            res['location']['y'],
            self.addr_col,
            add
          )
          conn.execute(query)