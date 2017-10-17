# https://developers.arcgis.com/python/
from arcgis.gis import GIS
from .utils import psql_to_geojson

# requires environment variables: AGO_USER, AGO_PASS
from os import environ as env
import os

gis = GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])

def upload(gis, filepath):
  item = gis.content.add({"type": "GeoJson"}, filepath)
  return item

def overwrite(gis, id, filepath):
  from arcgis.features import FeatureLayerCollection
  item = gis.content.get(id)
  flc = FeatureLayerCollection.fromitem(item)
  flc.manager.overwrite(filepath)
  return item

class AgoLayer(object):
  def __init__(self, params):
    self.params = params
    print(params)

  def publish(self):
    psql_to_geojson(self.params['table'], self.params['file'])
    if not self.params['id']:
      item = upload(gis, self.params['file'])
      os.system("rm {}".format(self.params['file']))
      self.item = item.publish()
    else:
      psql_to_geojson(self.params['table'], self.params['file'])
      overwrite(gis, self.params['id'], self.params['file'])
    os.system("rm {}".format(self.params['file']))

