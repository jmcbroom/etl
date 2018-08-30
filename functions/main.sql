/*
Postgres functions for common transform steps and origin-specific formatting quirks
*/

/*
Formats a postgres timestamp so it can be loaded as a socrata calendar_date or floating_timestamp type
Param: timestamp - origin system date value
Returns: text - ISO8601 time string accurate to the second with no timezone offset
Example usage: (select makeSocrataDate(csm_recd_date)) as csm_recd_date
*/
create or replace function makeSocrataDate (sys_dt timestamp) returns text as $$
    select replace(sys_dt::text, ' ', 'T')
$$ language sql;

/*
Formats NaN as nulls; 'NaN' is an artifact of pandas used for null float values that causes import errors
Param: float - origin system float value
Returns: float - original value or null
*/
create or replace function handleNan (sys_num float) returns float as $$
    select case when sys_num = 'NaN' then null else sys_num end
$$ language sql;

/*
Formats a postgis geometry so it can be loaded as a socrata location or point type
Param: geometry - https://postgis.net/docs/geometry.html
Returns: text - rounded latitude, longitude coordinate in 4326 projection
*/
create or replace function makeSocrataLocation (geom geometry) RETURNS text AS $$
    select case 
        when geom is null 
        then null
        else concat('location (', round(st_y(st_transform(geom, 4326))::numeric, 5), ',', round(st_x(st_transform(geom, 4326))::numeric, 5), ')') 
    end
$$ LANGUAGE sql;
