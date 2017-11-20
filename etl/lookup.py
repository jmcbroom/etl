import pandas
import re
import csv
import sqlalchemy
from os import environ as env
from .utils import connect_to_pg, df_to_pg

def read_file(path):
    """ read an external csv into new postgres table """
    df = pandas.read_csv("{}/{}".format(env['FILEROOT'], path))
    conn = connect_to_pg()
    conn.execute("drop table if exists pubsafe.misfits cascade;")
    df_to_pg(df, 'pubsafe', 'misfits')

def create_flag():
    """ create new column to indicate if row has been anonymized yet, set to false by default """
    conn = connect_to_pg()
    conn.execute("alter table {} add column is_anon boolean".format(self.table))
    conn.execute("update {} set is_anon = {}".format(self.table, self.set_flag))

def lookup_address():
    """ if incident address is place with known address, replace with street address before anonymization, set_flag remains false """
    conn = connect_to_pg()
    query = "update pubsafe.cad_update c set incident_address = geoc_address from pubsafe.misfits m where c.incident_address = m.unique_misfit"
    conn.execute(query)

def accept_place():
    """ accept common place names for incident address (eg 'Woodward Ave'), set_flag is true """
    conn = connect_to_pg()
    query = "update pubsafe.cad_update c set incident_address = anon_return, is_anon = 'true' from pubsafe.misfits m where c.incident_address = m.unique_misfit"
    conn.execute(query)

def redact_location():
    """ redact incident address up front if contains key words or starts with @, set_flag is true """

class LookupValues(object):
  def __init__(self, table='cad_update', field='incident_address', file='misfits_lookup.csv', on='match', set_flag='false'):
    self.table = table
    self.field = field
    self.file = file
    self.on = on
    self.set_flag = set_flag
