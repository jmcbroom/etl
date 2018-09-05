create or replace function search_parcels (search text)
    returns setof parcels as $$
    select * from parcels 
    where parcelno ilike ('%' || search || '%')
$$ language sql stable;

create or replace function search_parcels_latlng (lat double precision, lng double precision, radius integer)
    returns setof parcels as $$
    select objectid,
        parcelno,
        shape_length,
        shape_area,
        wkb_geometry,
        address,
        st_distance(
            st_transform(st_setsrid(st_makepoint(lng, lat), 4326), 2898), 
            st_centroid(wkb_geometry)) 
        as distance 
    from parcels 
    where st_dwithin(st_centroid(wkb_geometry), st_transform(st_setsrid(st_makepoint(lng, lat), 4326), 2898), radius)
    order by distance asc
$$ language sql stable;

create function parcel_events (search text)
    returns setof events as $$
    select 'property sale' as type, saledate::timestamp as date, concat(grantor, ' to ', grantee, ' for $', saleprice) as description, search as parcelno from sales_clean where pnum = search
        union all
    select 'building permit' as type, left(permit_issued, 19)::timestamp as date, bld_permit_type as description, search as parcelno from permits_clean where parcel_no = search
        union all
    select 'blight violation' as type, violation_date::timestamp as date, violation_description as description, search as parcelno from bvn_clean where parcelno = search
    order by date asc
$$ language sql stable;
