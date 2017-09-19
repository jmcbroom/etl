-- side lots
create table dlba.side_lots as (select 
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
and pb.buyer_status = 'Selected');

-- own it now
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
and pb.buyer_status = 'Selected');

-- all sold
