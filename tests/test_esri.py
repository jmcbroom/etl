from .context import esri
import unittest

class GeocoderTest(unittest.TestCase):
    def test(self):
        coder = esri.Geocoder()

        # test a bad address
        self.assertEqual(coder.geocode("123 Fake Street"), None)

        # test a known address: Grande Ballroom parcel id
        grande = coder.geocode("8952 Grand River")
        self.assertEqual(grande['attributes']['User_fld'], '14001590.')

        # test a known non-address to match it:
        maybe = coder.geocode("328 Hendrie")
        self.assertEqual(coder.match_to_parcel("328 Hendrie", maybe)['attributes']['parcel_id'], "01001577.")

        # test intersections
        packard = coder.geocode("concord and east grand")
        self.assertEqual(packard['address'], "Concord St & E Grand Blvd, Detroit, 48211")
        livernois = coder.geocode("7 mi and livernois")
        self.assertEqual(livernois['address'], 'W 7 Mile Rd & Livernois Ave, Detroit, 48221')
