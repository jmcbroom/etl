# https://developers.arcgis.com/python/
from arcgis.gis import GIS

# requires environment variables: AGO_USER, AGO_PASS
from os import environ as env

gis = GIS("https://detroitmi.maps.arcgis.com", env['AGO_USER'], env['AGO_PASS'])

print(gis)

class Layer(object):
  def __init__(self, params):
    pass
