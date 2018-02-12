from .utils import connect_to_pg, psql_to_geojson
import sys, os

class Mapbox(object):
    def __init__(self, params):
        self.config = params
    
    def upload(self):
        # dump to GeoJSON
        psql_to_geojson(self.config['table'], "{}.json".format(self.config['tileset']))

        # make tippecanoe options
        make_tiles = "/home/gisteam/tippecanoe/tippecanoe -o {}.mbtiles {}.json".format(self.config['tileset'], self.config['tileset'])
        print(make_tiles)
        os.system(make_tiles)

        upload = "mapbox-upload cityofdetroit.{} {}.mbtiles".format(self.config['tileset'], self.config['tileset'])
        print(upload)
        os.system(upload)

        remove = "rm ./{}.*".format(self.config['tileset'])
        print(remove)
        os.system(remove)

