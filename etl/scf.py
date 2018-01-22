import requests
import arrow
from urllib.parse import urlencode
from os import environ as env
import json
import pandas
import collections
from .utils import df_to_pg
from pprint import pprint

SCF_API = "https://seeclickfix.com/api/v2/organizations/507/issues?"

fieldnames = [
    'id',
    'status',
    'request_type_title',
    'summary',
    'description',
    'lat',
    'lng',
    'address',
    'created_at',
    'acknowledged_at',
    'closed_at',
    'reopened_at',
    'updated_at',
    'report_method',
    'priority_code', 
    'canonical_issue_id',
    'html_url',
    'issue_closed_comment'
]

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def flatten_comment(record):
    """Create single key/value dict from array of comments"""
    flat_comment = {}
    comment = record['comments']
    for c in comment:
        if c['comment_type'] == "Issue Closed":
            flat_comment['issue_closed_comment'] = c['comment']
        else:
            flat_comment['issue_closed_comment'] = ""
    return flat_comment

def clean(record):
  record_to_return = {}
  for k in fieldnames:
    if k in record.keys():
      record_to_return[k] = record[k]
  return record_to_return

def get_max_update_dt():
  """Ask the `scf.issues` table for the `max(updated_at)` value"""
  from .utils import connect_to_pg
  conn = connect_to_pg()
  query = "select max(updated_at) from scf.issues"
  res = conn.execute(query)
  max_dt = res.fetchone()[0]
  print(max_dt)
  return max_dt

class Seeclickfix(object):
  """Get SCF data for the last day or so."""

  def __init__(self):
    after_date = get_max_update_dt()
    params = {
      "updated_at_after": after_date,
      "status": "open,acknowledged,closed,archived"
    }
    url = SCF_API + urlencode(params)
    print(url)

    req = requests.get(url, auth=(env['SCF_USER'], env['SCF_PASS']))
    initial_json = json.loads(req.text)

    self.pages = initial_json['metadata']['pagination']['pages']
    self.record_count = initial_json['metadata']['pagination']['entries']
    print("Expecting {} records".format(self.record_count))

    self.records = initial_json['issues']
    next_page_url = initial_json['metadata']['pagination']['next_page_url']

    while len(self.records) < self.record_count:
      print(next_page_url)
      req = requests.get(next_page_url, auth=(env['SCF_USER'], env['SCF_PASS']))
      resp = json.loads(req.text)
      self.records += resp['issues']
      next_page_url = resp['metadata']['pagination']['next_page_url']

    self.records = [ flatten(r) for r in self.records ]
    self.records = [ flatten_comment(r) for r in self.reords ]
    self.records = [ clean(r) for r in self.records ]

  def to_postgres(self):
    self.df = pandas.DataFrame.from_records(self.records)
    # "delete from scf.issues_update where id in ({})".format(str(",".join([r['id'] for r in self.records])))
    df_to_pg(self.df, 'scf', 'issues_update')
