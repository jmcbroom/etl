-- side lots sold
create table dlba.side_lots as (
select 
a.actual_closing_date,
a.sale_status, 
c.address, 
c.parcel_id,
c.program, 
c.neighborhood, 
c.council_district, 
c.acct_latitude, 
c.acct_longitude, 
pb.buyer_status,
pb.final_sale_price,
pb.purchaser_type
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
where a.recordtypeid = '012j0000000xtGvAAI' 
and a.actual_closing_date is not null
and pb.buyer_status = 'Selected'
and c.address not like 'Fake%'
);

-- own it now sold
create table dlba.own_it_now as (
select 
a.actual_closing_date,
a.sale_status, 
c.address, 
c.parcel_id,
c.program, 
c.neighborhood, 
c.council_district, 
c.acct_latitude, 
c.acct_longitude, 
pb.buyer_status,
pb.final_sale_price,
pb.purchaser_type
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
where a.dlba_activity_type in ('Demo Pull Sale','Demo Pull for Demo Sale','Own It Now','Own It Now - Bundled Property')
and a.actual_closing_date is not null
and pb.buyer_status = 'Selected'
and c.address not like 'Fake%'
);

-- auction sold
create table dlba.auction_sold as (
select 
a.actual_closing_date,
a.sale_status, 
c.address, 
c.parcel_id,
'Auction' as program, 
c.neighborhood, 
c.council_district, 
c.acct_latitude, 
c.acct_longitude, 
pb.buyer_status,
pb.final_sale_price,
pb.purchaser_type
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
where a.recordtypeid = '012j0000000xtGoAAI' 
and a.sale_status = 'Closed'
and pb.buyer_status = 'Selected'
and c.address not like 'Fake%'
);

-- for sale
create table dlba.for_sale as (
select
c.address,
c.parcel_id,
c.program,
a.listing_date
c.neighborhood,
c.council_district,
c.acct_latitude,
c.acct_longitude
from dlba.dlba_activity a
inner join dlba.case c on a.case = c.id
where (a.recordtypeid = '012j0000000xtGoAAI'
or a.dlba_activity_type in ('Demo Pull Sale', 'Demo Pull for Demo Sale', 'Renovation Sale', 'Own It Now', 'Own It Now - Bundled Property', 'Auction - Bundled Property'))
and a.listing_date < now()
and a.sale_status = 'For Sale On Site'
and c.status = 'For Sale'
and c.address not like 'Fake%'
);
