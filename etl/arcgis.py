# https://developers.arcgis.com/python/
from arcgis.gis import GIS
from .utils import psql_to_geojson

# requires environment variables: AGO_USER, AGO_PASS
from os import environ as env
import os

gis = GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])

def upload(gis, filepath, params):
  item_properties ={'title':params['title'],
            'description':params['description'],
            'tags':','.join(params['tags']),
            'type': 'GeoJson'}
  item = gis.content.add(item_properties, filepath)
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
      item = upload(gis, self.params['file'], self.params)
      self.item = item.publish()
      # to do: write back new item id to .yml file
    else:
      overwrite(gis, self.params['id'], self.params['file'])
    # remove GeoJSON file
    os.system("rm {}".format(self.params['file']))

