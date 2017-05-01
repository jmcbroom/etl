import os
import requests, json, csv

SCF_USER = os.environ['SCF_USER']
SCF_PASS = os.environ['SCF_PASS']

fieldnames = [
    'id',
    'status',
    'closed_at',
    'comments_count',
    'comment_type_issue_closed'
]

def flatten_comment(issue):
    """ Create single key/value dict from array of dicts """
    flat_comment = {}
    comment = issue['comments']
    for c in comment:
        if c['comment_type'] == "Issue Closed":
            flat_comment['comment_type_issue_closed'] = c['comment']
        else:
            pass
    return flat_comment

def scf_get_features(url):
    """ Create list of dicts, where each dict is a single scf issue """
    features = []
    ask = requests.get(url, auth=(SCF_USER, SCF_PASS))
    resp = json.loads(ask.text)
    for i in resp['issues']:
        # first, flatten comments list to a dict and add to i
        a = flatten_comment(i)
        i.update(a.copy())

        # then, make a new dict from i with only the fields we care about (we can join back to full issue data by id later)
        new_i = {k: i[k] for k in set(fieldnames) & set(i.keys())}
        features.append(new_i)
    return features

def scf_pagination(url):
    """ Get metadata """
    ask = requests.get(url, auth=(SCF_USER, SCF_PASS))
    resp = json.loads(ask.text)
    return resp['metadata']['pagination']

if __name__ == "__main__":
    scf_org_api = 'http://seeclickfix.com/api/v2/organizations/507/issues?per_page=20&status=closed'
    paginate = scf_pagination(scf_org_api)
    features = []

    # fetch all pages of issues
    for i in range(1, 3 + 1, 1):
        print("Fetching page {} of {}".format(i, paginate['pages']))
        try:
            page_features = scf_get_features(scf_org_api + '&page={}'.format(i))
            features = features + page_features
        except:
            print("Failed on page {}".format(i))
            pass
            
    # write to a csv, where each row is an issue
    with open('scf_issues_with_comments.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for f in features:
            try:
                writer.writerow(f)
            except:
                print(f)
                pass
