import arcgis, os, json, requests, time
from datetime import datetime, timedelta

gis = arcgis.gis.GIS("https://www.arcgis.com", os.environ['AGO_USER'], os.environ['AGO_PASS'])

## get a list of Parcel IDs that have been surveyed
# placeholder for parcel id's
pids = []
inspections = arcgis.features.FeatureLayer("http://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/service_899dd1d063da4ceea057e2460a9824c2/FeatureServer/0", gis=gis)
two_days_ago = datetime.today() - timedelta(days=5)
where_clause = "1=1 AND CreationDate > date '{}'".format(two_days_ago.strftime("%m/%d/%Y"))
inspection_count = inspections.query(where=where_clause, return_count_only=True)

# loop through and offset
for i in range(0, inspection_count, 1000):
    result = inspections.query(where=where_clause,out_fields='parcelnumber',return_geometry=True, result_offset=i, result_record_count=1000)
    ids = [r.attributes["parcelnumber"] for r in result.features]
    pids = pids + ids

## update the original parcel layer to mark Done
base_commercial_parcels = arcgis.features.FeatureLayer("http://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/DPW_SolidWaste_Parcels/FeatureServer/0", gis=gis)
calc_expression = {"field": "inspection_done", "value": "1"}
for p in pids:
    try:
        print("Updating {}".format(p))
        base_commercial_parcels.calculate("parcelno = '{}'".format(p), calc_expression)
    except:
        pass

# get done count
done_count = base_commercial_parcels.query(where='inspection_done=1', return_count_only=True)

## post a message to Slack with the results
webhook_url = webhook_url = 'https://hooks.slack.com/services/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'
message = "Solid Waste Inspections update for {}: {} parcels marked as done :ballot_box_with_check:".format(time.strftime("%m/%d"), done_count)
slack_data = {'text': message}

response = requests.post(
    webhook_url, data=json.dumps(slack_data),
    headers={'Content-Type': 'application/json'}
)
if response.status_code != 200:
    raise ValueError(
        'Request to slack returned an error %s, the response is:\n%s'
        % (response.status_code, response.text)
    )
