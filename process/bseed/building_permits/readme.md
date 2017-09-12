# Building Permits

## FME Workflow

Tidemark view: `BG_BLD_DET_F`

where: `TRUNC(PERMIT_ISSUED) > $(YESTERDAY)`

- remove duplicates on `PERMIT_NO`

- address cleanup

- join to parcels

- add column if geocoded

## Reverse engineering the above view

Likely tables we'll need:

- `CASEMAIN` (base table) `where "CSM_CASENO" like 'BLD%'`
    - general case info
    - `PRC_PARCEL_NO`: send to tidemark cleanup
    - `CSM_EXPR_DATE`
    - `CSM_FINALED_DATE`
    - `CSM_ISSUED_DATE`
- `CASE_BLD` (joins on `CSM_CASENO`)
    - contains building permit info
    - `BLD_PERMIT_TYPE`: lookup 4-character code
    - `BLD_PERMIT_DESC`: as is
    - `BLD_USE_TYPE`: lookup in `VALIDATION_VALUES` where `VALIDKEY = 'bld_use_code'`
    - `BLD_TYPE_CONST_COD`: ?? (look up somewhere)
    - `BLD_STORIES`: as is
    - `BLD_NO_UNITS`: as is
    - `BLD_EST_COST`: as is
    - `BLD_LEGAL_USE`: legal use
    - lookup:
        - `BLD_USE_TYPE` in `VALIDATION_VALUES`
- `CASE_PEOPLE` (join on `CSM_CASENO`)
    - contains contractor/owner address information
    - join on `CSM_CASENO`
    - `ROLE_TYPE`: 4 
        - `TAX`
        - `CON`
        - `OWN`
        - `APL`
