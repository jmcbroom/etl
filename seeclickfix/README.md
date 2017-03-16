## SeeClickFix

Workflow for fetching Improve Detroit issues from the SeeClickFix API, publishing them as a Socrata dataset, and doing analysis.

### Context

The Improve Detroit app is built on the SeeClickFix platform, which makes data about issues available through two APIs: a public endpoint and an organization endpoint that requires authentication. The organization enpoint gives us more fields for the same set of issues.

We publish issues from the public endpoint on our Socrata Open Data Portal, found [here](https://data.detroitmi.gov/Government/Improve-Detroit-Submitted-Issues/fwz3-w3yn).

Different city departments use the issue data to track various metrics on a monthly or weekly basis.

### SeeClickFix -> Socrata

How we get data from the APIs into Socrata.
 
#### Public endpoint

We follow the process outlined in [Socrata's Connectors & ETL Templates repository](https://socrata.github.io/connectors/templates/see-click-fix-to-socrata.html). This workflow relies on a python script, FME Workbench (a visual editor for data integration), and the Socrata Writer.

#### Organization endpoint

We wrote our own python script to flatten nested data fields, paginate through the issues, and write them to a .csv that we can upload as a Socrata dataset. This one doesn't use FME Workbench.

Run it (using python 3.6): `python scf_to_csv.py`

### Analysis

Some stats we're interested in tracking:
- How many issues of each type are opened each week or month?
- How many days pass from when an issue is created to when it's closed? Is this time within the Service Level Agreements different departments commit to?
- What types of issues are most likely to be re-opened? To be duplicates?

`stats.ipynb` has a sample of these calculations using pandas and numpy. It builds a dataframe from the .csv we generate from the organization endpoint above.

### Next steps

- `scf_to_csv.py` currently grabs all issues, which we want to create our initial dataset. But from then on, we want to scrape just the new or updated issues every day and add them to the existing dataset. We're still figuring out the best way to capture and fetch only what's changed
- document how we configure DataSync and Windows Task Manager to automatically run this workflow once a day
- the current model for analysis pulls stats for a week at a time, but we know there's interest in extending it to more easily compare metrics across time, like generating stats for the last four weeks in a row at once
- we should also adjust pandas to read/query the initial data as json directly from Socrata instead of importing a .csv
- add test coverage
