import pandas
import re
import sqlalchemy
from os import environ as env
from .utils import connect_to_pg, df_to_pg

conn = connect_to_pg()

def read_file(path):
    """ read an external csv into new postgres table """
    df = pandas.read_csv("{}/{}".format(env['FILEROOT'], path))
    conn.execute("drop table if exists pubsafe.misfits cascade;")
    df_to_pg(df, 'pubsafe', 'misfits')

def redact_location(loc):
    """ redact incident address up front if contains key words or starts with @ """
    redactors = [
        'ARMED', 
        'ASSAULT', 
        'ASSIST', 
        'AUTO', 
        'CARJACKING',
        'DEATH', 
        'DESTRUCTION', 
        'FATAL', 
        'FELONIOUS', 
        'INVESTIGATION', 
        'KIDNAPPING', 
        'LARCENY', 
        'LOOK OUT', 
        'MISSING', 
        'NARCOTICS',
        'REMARKS',
        'REPORT',
        'ROBBERY',
        'SERIOUS',
        'SHOOTING',
        'SHOT',
        'SPEC ATTEN',
        'SPEC ATTN',
        'SPECIAL ATTENTION',
        'STABBED',
        'UNIT',
        'WEAPON',
        'chasing']

    redactor = re.findall(r"(?=("+'|'.join(redactors)+r"))", loc)
    starter = "@"

    if len(redactor) > 0 or loc.startswith(starter):
        loc = "Location Redacted"
    else:
        pass

    return loc

class LookupValues(object):
    def __init__(self, table='pubsafe.cad_update', field='incident_address', file='misfits_lookup.csv', on='match', set_flag='false'):
        self.table = table
        self.field = field
        self.file = file
        self.on = on
        self.set_flag = set_flag

    def lookup(self):
        # find known street addresses for incident places, is anonymized remains false
        addr_lookup = """update {} c 
                        set {} = geoc_address 
                        from pubsafe.misfits m 
                        where c.{} = m.unique_misfit
                        and m.geoc_address is not null""".format(self.table, self.field, self.field)
        
        # accept common names for incident places, is anonymized is true
        place_lookup = """update {} c 
                        set {} = anon_return, is_anon = 'true' 
                        from pubsafe.misfits m 
                        where c.{} = m.unique_misfit
                        and m.anon_return is not null""".format(self.table, self.field, self.field)
        
        # redact bad words before anonymizing, is anonymized is true
        redaction_lookup = """update {}
                            set {} = {}, is_anon = 'true'
                            where {} = {}""".format(self.table, self.field, redact_location(v), self.field, v)

        for q in [addr_lookup, place_lookup, redact_location]:
            print(q)
            conn.execute(q)
