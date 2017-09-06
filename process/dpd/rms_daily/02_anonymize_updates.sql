-- after loading pandas.df to PostgreSQL:
-- add geometry column & create an index on that column
alter table rms_update add column uniq serial primary key;
select addgeometrycolumn('rms_update', 'geom', 2898, 'POINT', 2);

-- indices
alter index if exists etl_rms_update_geom_idx rename to etl_rms_geom_idx;
create index etl_rms_update_geom_idx on rms_update using gist(geom);
drop index if exists etl_rms_crimeid_idx;
create index etl_rms_crimeid_idx on rms using btree(dpdcrimeid);
drop index if exists etl_rms_update_crimeid_idx;
create index etl_rms_update_crimeid_idx on rms using btree(dpdcrimeid);

-- make a point from X and Y, then drop columns
update rms_update set geom = st_setsrid(st_makepoint(x, y), 2898);
alter table rms_update drop column x;
alter table rms_update drop column y;

-- need to stash the unique ID of the closest road
alter table rms_update add column close_rd_gid bigint;
update rms_update r set close_rd_gid =
	(select c.gid from base.centerline c
		where st_dwithin(r.geom, c.geom, 250)
		order by st_distance(r.geom, c.geom) asc limit 1);

-- add geoid from census blocks
alter table rms_update add column block_id varchar(50);
update rms_update r set block_id = b.geoid10 from base.blocks_2010 b where st_contains(b.geom, r.geom);

-- add neighborhood
alter table rms_update add column neighborhood varchar(100);
update rms_update r set neighborhood = n.name from base.neighborhoods n where st_contains(n.wkb_geometry, r.geom);

-- add council district
alter table rms_update add column council_district integer;
update rms_update r set council_district = d.districts::integer from base.council_districts d where st_contains(d.wkb_geometry, r.geom);

-- add zip code
alter table rms_update add column zip_code varchar(10);
update rms_update r set zip_code = z.zipcode from base.zip_codes z where st_contains(z.wkb_geometry, r.geom);

-- grab existing geometry from rms
update rms_update set geom = (select geom from rms where rms_update.dpdcrimeid = rms.dpdcrimeid limit 1);

-- for those that didn't have existing geom, create new point
update rms_update r set geom =
	(st_dump( -- get Point from MultiPoint
		st_generatepoints(
			st_intersection(
				st_buffer(c.geom, 30), -- 30 feet from the road
				st_buffer(st_closestpoint(c.geom, r.geom), 150) -- 150 feet from the snapped point
			), 1)
	)).geom
	from base.centerline c where r.close_rd_gid = c.gid and r.geom is null;


-- drop road col
alter table rms_update drop column close_rd_gid;

-- add x and y back in
alter table rms_update add column lon double precision;
alter table rms_update add column lat double precision;
update rms_update set lon = round(ST_X(ST_Transform(geom, 4326))::numeric, 5);
update rms_update set lat = round(ST_Y(ST_Transform(geom, 4326))::numeric, 5);
