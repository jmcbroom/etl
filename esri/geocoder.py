import json, requests, re
import Levenshtein
import fire
from urllib.parse import urlencode

ROOT = "https://gis.detroitmi.gov/arcgis/rest/services"

ENDPOINTS = {
    "composite": "/DoIT/CompositeGeocoder/GeocodeServer/",
    "point": "/DoIT/AddressPointGeocoder/GeocodeServer/",
    "street": "/DoIT/StreetCenterlineGeocoder/GeocodeServer/",
    "addresses": "/Base/Addresses/FeatureServer/0"
}

OPTS = {
        'outSR': 4326,
        'outFields': '*',
        'returnGeometry': 'false',
        'f': 'pjson'
    }

def split(address):
    """Split addresses and input into numbers and the street name"""
    m = re.match('(^[0-9]+)\s([0-9a-zA-Z\s]+)', address)
    if m:
        return m.group(1), m.group(2)
    else:
        return None

class Geocoder(object):
    """An Esri geocoder."""

    def __init__(self, url='{}{}'.format(ROOT,ENDPOINTS['composite'])):
        """Return a Geocoder object whose url is *url*"""
        self.url = url

    def geocode(self, address, opts=OPTS):
        """Return one geocoding result."""
        opts['SingleLine'] = address
        params = urlencode(opts)
        req = requests.get('{}{}{}'.format(self.url,'/findAddressCandidates?',params))
        response = json.loads(req.text)
        if response['candidates'] and len(response['candidates']) > 0:
            return response['candidates'][0]
        else:
            return None

    def match_to_parcel(self, address, result):
        # lookup coords against address
        opts = {
            'geometry': "{x},\r\n{y}".format(**result['location']),
            'geometryType': 'esriGeometryPoint',
            'spatialRel': 'esriSpatialRelIntersects',
            'inSR': 4326,
            'outSR': 4326,
            'distance': 250,
            'units': 'esriSRUnit_Foot',
            'returnGeometry': 'true',
            'outFields': 'house_number, street_dir_prefix, street_name, street_type, street_dir_suffix, parcel_id',
            'f': 'pjson'
        }
        params = urlencode(opts)
        req = requests.get('{}{}{}{}'.format(ROOT, ENDPOINTS['addresses'], '/query?', params))
        response = json.loads(req.text)
        # if there were matches, loop through and compare
        if response['features'] and len(response['features']) > 0:
            best_match = None
            distance_to_beat = 2000
            if split(address) == None:
                return None
            house_num, street_name = split(address)
            for f in response['features']:
                in_num, in_name = f['attributes']['house_number'], f['attributes']['street_name']
                num_diff = abs(int(in_num) - int(house_num))
                str_similar = Levenshtein.ratio(in_name.upper(), street_name.upper())

                # closer house number
                # & same side of street
                # & street name is in the ballpark?
                # it's the new best match
                if num_diff <= distance_to_beat and num_diff % 2 == 0 and str_similar > 0.5:
                    best_match = f
                    distance_to_beat = num_diff
                else:
                    pass
        if best_match:
            return best_match

if __name__ == "__main__":
    fire.Fire(Geocoder)
