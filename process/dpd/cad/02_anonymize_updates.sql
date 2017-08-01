-- after loading pandas.df to PostgreSQL:
-- add primary key, geometry column & create an index on that column
-- alter table cad_update add column gid serial primary key;
select addgeometrycolumn('cad_update', 'geom', 2898, 'POINT', 2);
create index etl_cad_update_geom_idx on cad_update using gist(geom);

-- make a point from X and Y, then drop columns
update cad_update set geom = st_setsrid(st_makepoint(geox, geoy), 2898) where geox > 0 and geoy > 0;

-- need to stash the unique ID of the closest road
alter table cad_update add column close_rd_gid bigint;
update cad_update r set close_rd_gid =
	(select c.gid from base.centerline c
		where st_dwithin(r.geom, c.geom, 250)
		order by st_distance(r.geom, c.geom) asc limit 1) where geom is not null;
   
-- add geoid from census blocks
alter table cad_update add column block_id varchar(50);
update cad_update r set block_id = b.geoid10 from base.blocks_2010 b where st_contains(b.geom, r.geom);

-- add neighborhood
alter table cad_update add column neighborhood varchar(100);
update cad_update r set neighborhood = n.name from base.neighborhoods n where st_contains(n.wkb_geometry, r.geom);

-- add council district
alter table cad_update add column council_district integer;
update cad_update r set council_district = d.districts::integer from base.council_districts d where st_contains(d.wkb_geometry, r.geom);

-- replace geom with the fuzzed point
update cad_update r set geom =
	(st_dump( -- get Point from MultiPoint
		st_generatepoints(
			st_intersection(
				st_buffer(c.geom, 30), -- 30 feet from the road
				st_buffer(st_closestpoint(c.geom, r.geom), 150) -- 150 feet from the snapped point
			), 1)
	)).geom
	from base.centerline c where r.close_rd_gid = c.gid and r.geom is not null;

-- drop road col
alter table cad_update drop column close_rd_gid;

-- add x and y back in
alter table cad_update add column lon double precision;
alter table cad_update add column lat double precision;
update cad_update set lon = round(ST_X(ST_Transform(geom, 4326))::numeric, 5) where geom is not null;
update cad_update set lat = round(ST_Y(ST_Transform(geom, 4326))::numeric, 5) where geom is not null;

update cad_update set geom = null, lat = null, lon = null where abs(ST_X(ST_Transform(geom, 4326)))::numeric < 1;
update cad_update set geom = null, lat = null, lon = null where abs(ST_Y(ST_Transform(geom, 4326)))::numeric < 1;
