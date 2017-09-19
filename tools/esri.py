import os, yaml, fire
from slack import SlackMessage

FILEROOT = os.environ['ETL_ROOT']

class FeatureLayer(object):
    def __init__(self, dir='dah/bvn'):
        self.directory = FILEROOT + 'process/' + dir
        with open(self.directory + "/config.yml", 'r') as f:
            self.conf = yaml.load(f)
        self.name = self.conf['name']
        self.view = self.conf['esri']['view']
        self.slug = self.conf['esri']['slug']
        self.proj = self.conf['epsg']
        if self.conf['esri']['columns']:
            self.cols = ",".join(self.conf['esri']['columns'])
        else:
            self.cols = '*'

    def send_update_msg(self):
        msg_txt = {
            "text": """Uploading to ArcGIS Online: *cityofdetroit.{}*""".format(self.slug)
        }
        self.msg = SlackMessage(msg_txt)
        self.msg.send()

    def to_geojson(self):
        to_geojson = """ogr2ogr -f GeoJSON {}.json -s_srs epsg:{}  -t_srs epsg:4326 -sql 'select {} from {}' pg:dbname={}""".format(
                                        self.slug,
                                        self.proj,
                                        self.cols, 
                                        self.view, 
                                        os.environ['PG_DB'])
        # return "{}.json".format(self.slug)
        print(to_geojson)
        return os.system(to_geojson)

if __name__ == "__main__":
    fire.Fire(FeatureLayer)