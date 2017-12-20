import os, yaml, fire
from slack import SlackMessage

FILEROOT = os.environ['ETL_ROOT']

class Tileset(object):
    def __init__(self, dir='assessor/parcels'):
        self.directory = FILEROOT + 'process/' + dir
        with open(self.directory + "/config.yml", 'r') as f:
            self.conf = yaml.load(f)
        self.name = self.conf['name']
        self.view = self.conf['mapbox']['view']
        self.slug = self.conf['mapbox']['slug']
        self.proj = self.conf['epsg']
        self.cols = ",".join(self.conf['mapbox']['columns'])

    def send_update_msg(self):
        msg_txt = {
            "text": """Uploading to Mapbox: *cityofdetroit.{}*""".format(self.slug)
        }
        self.msg = SlackMessage(msg_txt)
        self.msg.send()

    def upload(self):
        self.send_update_msg()
        to_geojson = """ogr2ogr -f GeoJSON {}.json -s_srs epsg:{}  -t_srs epsg:4326 -sql 'select {} from {}' pg:dbname={}""".format(
                                        self.slug,
                                        self.proj,
                                        self.cols, 
                                        self.view, 
                                        os.environ['PG_DB'])
        to_mbtiles = "/home/gisteam/tippecanoe/tippecanoe -o {}.mbtiles --layer {} {}.json".format(self.slug, self.slug, self.slug)
        to_mapbox = "mapbox-upload cityofdetroit.{} {}.mbtiles".format(self.slug, self.slug)
        remove = "rm {}.json; rm {}.mbtiles".format(self.slug, self.slug)
        print(to_geojson)
        os.system(to_geojson)
        print(to_mbtiles)
        os.system(to_mbtiles)
        self.msg.react_custom("1234")
        print(to_mapbox)
        os.system(to_mapbox)
        self.msg.react_custom("up")
        print(remove)
        os.system(remove)

if __name__ == "__main__":
    fire.Fire(Tileset)