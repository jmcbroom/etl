from .utils import connect_to_pg
import re
import math

connection = connect_to_pg()

def anonymize_text_location(loc):
  # trim the loc
  loc = loc.strip()

  # define separators
  separators = ["/", "&", "@", " AND ", " AT "]
  # define regex for leading number
  num_pattern = re.compile(r'^[0-9]+')
  str_pattern = re.compile(r"\s?([A-Za-z\s0-9\/&-.@]+)")

  num_match = num_pattern.match(loc)

  if len(str_pattern.findall(loc)) > 0:
    str_match = str_pattern.findall(loc)[0].strip()
  else:
    str_match = loc

  # if there's a seperator in the street regex, it's an intersection
  separator = re.findall(r"(?=("+'|'.join(separators)+r"))", str_match)
  if len(separator) > 0:
    if str_match and separator[0] in str_match:
      split = str_match.split(separator[0])
      split[0] = re.sub('^[0-9]+\s', "", split[0]).strip()
      return "Corner of {} and {}".format(split[0], split[1]).strip()
  elif num_match:
    num = int(num_match.group())
    blocknum = math.floor(num/100)*100
    if blocknum == 0:
        blocknum = 100
    # this is hacky and could be fixed with better str_match regex
    str_match = re.sub('^[0-9]+', "", str_match).strip()
    return "{} block of {}".format(blocknum, str_match).strip()

class AnonTextLocation(object):
  def __init__(self, table, column, set_flag=True):
    self.table = table
    self.column = column
    self.set_flag = set_flag

  def anonymize(self):
    # get all locs from table
    query = "select {} from {}".format(self.column, self.table)
    rows = connection.execute(query).fetchall()
    if self.set_flag:
      update = "update {} set {} = '{}', is_anon = 't' where {} = '{}'"
    else:
      update = "update {} set {} = '{}' where {} = '{}'"
    for r in rows:
      v = r[0]
      update_row_query = update.format(self.table, self.column, anonymize_text_location(v), self.column, v)
      connection.execute(update_row_query)

class AnonGeometry(object):
  def __init__(self, table, against):
    self.table = table
    self.against = against

  def anonymize(self):

    # Add a 
    add_column = "alter table {} add column close_rd_gid bigint".format(self.table)

    close_rd = """update {} r set close_rd_gid =
                      (select c.gid from {} c
                        where st_dwithin(r.geom, c.geom, 250)
                    order by st_distance(r.geom, c.geom) asc limit 1) 
                    where geom is not null""".format(self.table, self.against)
    
    anonymize_geom = """update {} r set geom =
                        (st_dump(
                        st_generatepoints(
                          st_intersection(
                            st_buffer(c.geom, 30),
                            st_buffer(st_closestpoint(c.geom, r.geom), 150) 
                          ), 1)
                        )).geom
                        from {} c 
                        where r.close_rd_gid = c.gid 
                        and r.geom is not null""".format(self.table, self.against)
    
    drop_close_rd = "alter table {} drop column close_rd_gid".format(self.table)

    for q in [add_column, close_rd, anonymize_geom, drop_close_rd]:
      print(q)
      connection.execute(q)
