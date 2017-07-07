import sqlalchemy, odo, pandas, os, re, math, csv

user = os.environ['CAD_USER']
pword = os.environ['CAD_PASS']
host = os.environ['CAD_HOST']
db = os.environ['CAD_DB']
tablename = "vw_DP_CAD"

cad_engine = sqlalchemy.create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, pword, host, db))
cad_connection = cad_engine.connect()

start_date = '2016-09-20'
pg_connstr = 'postgres+psycopg2://{}@localhost/{}'.format(os.environ['PG_USER'], os.environ['PG_DB'])
etl_engine = sqlalchemy.create_engine(pg_connstr)
etl_connection = etl_engine.connect()

max_inciid_result = etl_connection.execute("select max(incident_id) from cad_socrata")
max_inciid = int(max_inciid_result.fetchone()[0])

print("Grabbing all records with `inci_id` > {}".format(max_inciid))

df = pandas.read_sql("select * from dbo.{} where inci_id >= '{}'".format(tablename, max_inciid), cad_connection)

print("Found {} new CAD records...".format(len(df)))

# upload the misfits_lookup csv of addresses to geocode or anonymize
reader = csv.reader(open("{}/misfits_lookup.csv".format(os.environ['FILEROOT'], "r")))

# define two dicts for field maps
anon_map = {}
geo_map = {}

# fill the dicts from non-empty rows, exclude header row
for row in reader:
    for rows in reader:
        misfit = rows[0]
        geo_return = rows[2]
        anon_return = rows[3]

        if geo_return:
            geo_map[misfit] = geo_return
        elif anon_return:
            anon_map[misfit] = anon_return
        else:
            pass

def geocode_misfits(value):
    """Geocode misfit addresses based on a field map before anonymization"""
    value = value.strip()
    if value in geo_map:
        geo_value = geo_map[value]
        return geo_value
    else:
        return value

# add new col with geocoded locations
df['geo_match_addr'] = df['incident_address'].apply(lambda x: geocode_misfits(x))

# list of words that appear in incident_address that trigger location redaction up front
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

# list of characters that separate intersections
separators = ["/", "&", "@", " AND ", " AT "]

# list to store addresses that don't cleanly map to blocks or intersections
misfits = []

def anonymize_location(value):
    """Anonymize location field: either a block or intersection."""
    # trim any leading or trailing whitespaces
    value = value.strip()

    # redact location up front if value contains key words or starts with @
    redactor = re.findall(r"(?=("+'|'.join(redactors)+r"))", value)
    starter = "@"
    if len(redactor) > 0 or value.startswith(starter):
        value = "Location Redacted"
        return value

    # regex to pull out housenumber and streets
    num_pattern = re.compile(r'^[0-9]+')
    str_pattern = re.compile(r"\s?([A-Za-z\s0-9\/&-.@]+)")
    num_match = num_pattern.match(value)

    if len(str_pattern.findall(value)) > 0:
        str_match = str_pattern.findall(value)[0]
    else:
        str_match = value

    # if there's a seperator in the street regex, it's an intersection
    separator = re.findall(r"(?=("+'|'.join(separators)+r"))", str_match)
    if len(separator) > 0:
        if str_match and separator[0] in str_match:
            split = str_match.split(separator[0])
            return "Corner of {} and {}".format(split[0], split[1]).strip()

    # or it's a XXX block of Y Street
    elif num_match:
        num = int(num_match.group())
        blocknum = math.floor(num/100)*100
        if blocknum == 0:
            blocknum = 100
        # this is hacky and could be fixed with better str_match regex
        str_match = re.sub('^[0-9]+', "", str_match)
        return "{} block of {}".format(blocknum, str_match).strip()

    # or it maps to an anonymous place
    elif value in anon_map:
        anon_value = anon_map[value]
        return anon_value

    # or it's still a misfit
    else:
        misfits.append(value)
        return "Location Redacted"

def trim_city(value):
    """Remove the city from the end of an address string if it exists"""
    # trim big spaces
    value = re.sub(r"[\s]{2,}", " ", value)
    
    # trim the city name or code off the end
    cities = [" DETROIT", " ALLE", " DEAR", " ECOR", " FERN", " GROS", " HAMT", " HAZE", " HIGH", " REDF", " RIVE", " ROYA", " SOUT", " WARR"]
    for c in cities:
        if value.endswith(c):
            value = value[:-len(c)]
        else:
            value = value
            
    # if theres an apt number, remove everything after APT
    apt = " APT "
    street_before_apt = value.split(apt, 1)[0]
    value = street_before_apt
    
    return value

# add new col with anonymized incident address
df['anonymized_addr'] = df['geo_match_addr'].apply(lambda x: trim_city(anonymize_location(x)))

# send it to postgres
odo.odo(df, 'postgresql://{}@localhost/{}::cad_update'.format(os.environ['PG_USER'], os.environ['PG_DB']))
