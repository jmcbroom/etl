import requests, json, csv
from flatten_dict import flatten

SCF_USER = env['SCF_USER']
SCF_PASS = env['SCF_PASS']

# flatten_dict helper
def underscore_reducer(k1,k2):
    if k1 is None:
        return k2
    else:
        return k1 + "_" + k2

# create list of names based on available fields from api/v2/organizations
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

def issue_to_dict(issue):
    flattened = flatten(issue, reducer=underscore_reducer)
    properties = {}
    for f in fieldnames:
        try:
            properties[f] = flattened[f]
        except KeyError:
            pass
    return properties

def scf_get_features(url):
    features = []
    ask = requests.get(url, auth=(SCF_USER, SCF_PASS))
    resp = json.loads(ask.text)
    for i in resp['issues']:
        features.append(issue_to_dict(i))
    return features

def scf_pagination(url):
    ask = requests.get(url, auth=(SCF_USER, SCF_PASS))
    resp = json.loads(ask.text)
    return resp['metadata']['pagination']

if __name__ == "__main__":
    scf_org_api = 'http://seeclickfix.com/api/v2/organizations/507/issues?per_page=20&status=open,closed,acknowledged,archived'
    paginate = scf_pagination(scf_org_api)
    features = []

    # fetch all pages of issues
    for i in range(1, paginate['pages'] + 1, 1):
        print("Fetching page {} of {}".format(i, paginate['pages']))
        try:
            page_features = scf_get_features(scf_org_api + '&page={}'.format(i))
            features = features + page_features
        except:
            print("Failed on page {}".format(i))
            pass
            
    # write to a csv, where each row is an issue
    with open('all_scf_issues.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for f in features:
            try:
                writer.writerow(f)
            except:
                print(f)
                pass
