## `smartsheets`

Tools for doing stuff with Smartsheets.

### SheetToAGO

**Doesn't work yet**

Uploads a Smartsheet to ArcGIS Online (via a pandas dataframe.)

flags:
- `sheet_id` from Smartsheet

```bash
python smartsheets/ago.py --sheet_id=815144965040004 df_to_featureservice
```

### SheetToSocrata

Uploads a Smartsheet as a Socrata dataset.

flags:
- `sheet_id` from Smartsheet

```bash
python smartsheets/socrata.py --sheet_id=815144965040004 create-dataset
# returns https://data.detroitmi.gov/dataset/qs6k-wd5n
python smartsheets/socrata.py --sheet_id=815144965040004 --socrata_id=qs6k-wd5n load-data
```

### EnrichSheetAddresses

Adds four columns to a Smartsheet with addresses in it:
- `Matched_ParcelID`
- `Matched_Address`
- `Matched_Latitude`
- `Matched_Longitude`

flags:
- `sheet_id` from Smartsheet
- `address_col`: name of address column to geocode

```bash
python smartsheet/enrich.py --sheet_id=815144965040004 --address_col=address geocode_rows
```

### Should also add:
- Smartsheet to ArcGIS Online Feature Service
