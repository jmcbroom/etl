import requests
import arrow
from urllib.parse import urlencode
from os import environ as env
import json
import pandas
from utils import df_to_pg

from pprint import pprint

SCF_API = "https://seeclickfix.com/api/v2/organizations/507/issues?"

fieldnames = [
    'id',
    'status',
    'summary',
    'description',
    'rating',
    'lat',
    'lng',
    'address',
    'created_at',
    'acknowledged_at',
    'closed_at',
    'reopened_at',
    'updated_at',
    'comments_count',
    'vote_count',
    'report_method',
    'url',
    'priority_code',
    'assignee_id',
    'assignee_role',
    'assignee_name',
    'active_service_request_status',
    'active_service_request_sla_expires_at',
    'active_service_request_last_assigned_at',
    'active_service_request_created_at',
    'active_service_request_accepted_at',
    'active_service_request_started_at',
    'active_service_request_resumed_at',
    'active_service_request_finished_at',
    'active_service_request_closed_at',
    'active_service_request_reopened_at',
    'active_service_request_assignee_role',
    'active_service_request_assignee_id',
    'active_service_request_assignee_name',
    'reporter_id',
    'reporter_name',
    'reporter_role',
    'agent_id',
    'agent_name',
    'agent_role',
    'request_type_id',
    'request_type_title',
    'canonical_issue_id'
]

import collections

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def clean(record):
  record_to_return = {}
  for k in fieldnames:
    if k in record.keys():
      record_to_return[k] = record[k]
  return record_to_return

class Seeclickfix(object):
  """Get SCF data for the last day or so."""

  def __init__(self):
    after_date = arrow.utcnow().shift(hours=-3)
    params = {
      "after": after_date.isoformat(),
      "status": "open,acknowledged,closed,archived"
    }
    url = SCF_API + urlencode(params)
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
    self.records = [ clean(r) for r in self.records ]

  def to_postgres(self):
    self.df = pandas.DataFrame.from_records(self.records)
    "delete from scf.submitted_issues where id in ({})".format(",".join([r['id'] for r in self.records]))
    df_to_pg(self.df, 'scf', 'submitted_issues')