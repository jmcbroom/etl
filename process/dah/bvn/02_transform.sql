
-- let's create our master table
drop table if exists dah_joined cascade;

create table dah_joined (
    ticket_id bigint,
    ticket_number text,
    agency_name text,
    inspector_name text,
    violator_name text,
    violation_street_number text,
    violation_street_name text,
    violation_zip_code text,
    violator_id bigint,
    mailing_address_str_number text,
    mailing_address_str_name text,
    city text,
    state text,
    zip_code text,
    non_us_str_code text,
    country text,
    violation_date timestamp,
    ticket_issued_time text,
    hearing_date timestamp,
    hearing_time text,
    violation_code text,
    violation_description text,
    disposition text,
    fine_amount double precision,
    admin_fee double precision,
    state_fee double precision,
    late_fee double precision,
    discount_amount double precision,
    clean_up_cost double precision,
    judgment_amount double precision,
    payment_amount double precision,
    balance_due double precision,
    payment_date timestamp,
    payment_status text,
    collection_status text,
    -- grafitti_status text,
    violation_address text,
    parcelno text,
    propaddr text,
    legaldesc text,
    latitude double precision,
    longitude double precision,
    location text);

-- create indices on helper tables. speeds up ze joins
create index if not exists dah_joined_zticket_idx on dah_joined using btree(ticket_id);
create index if not exists dah_violator_info_idx on dah_violator_info using btree("ZTicketID");
create index if not exists dah_violator_address_idx on dah_violator_address using btree("ViolatorID");
create index if not exists dah_payment_id_idx on dah_payments using btree("ZticketID");

insert into dah_joined (
    ticket_id,
    ticket_number,
    agency_name,
    inspector_name,
    violation_street_number,
    violation_street_name,
    violation_zip_code,
    violator_id,
    violation_date,
    ticket_issued_time,
    hearing_date,
    hearing_time,
    violation_code,
    violation_description,
    fine_amount,
    late_fee,
    admin_fee,
    state_fee,
    discount_amount,
    clean_up_cost
)
select
    "ZTicketID",
    "TicketNumber",
    -- agency_name
    (select "AgencyName" from dah_agency ag where ag."AgencyID" = z."AgencyID"),
    -- inspector_name
    (select concat_ws(' ', s."UserFirstName", s."UserLastName") from dah_security s where z."ZTicketUserID" = s."SecurityID"),
    -- violation_street_number
    "ViolStreetNumber",
    -- violation_street_name
    (select "StreetName" from dah_streets s where s."StreetID" = z."ViolStreetName"),
    -- violation_zip_code
    "ViolZipCode",
    (select "FileID" from dah_violator_info vi where vi."ZTicketID" = z."ZTicketID" ),
    "IssueDate"::timestamp,
    "IssueTime",
    -- court_date - account for rescheduling
    (CASE
    WHEN ("ZTicketID" in (select "ZTicketID" from dah_reschedule)) THEN (select "ReScheduleDate" from dah_reschedule r where z."ZTicketID" = r."ZTicketID" order by "ReScheduleDate" desc limit 1)
    ELSE "CourtDate"
    END),
    -- court_time
    (CASE 
      WHEN "CourtTime" = 1.0 THEN '9:00 AM'
      WHEN "CourtTime" = 2.0 THEN '10:30 AM'
      WHEN "CourtTime" = 3.0 THEN '1:30 PM'
      WHEN "CourtTime" = 4.0 THEN '3:00 PM'
      WHEN "CourtTime" = 5.0 THEN '6:00 PM'
      ELSE null
    END),
    -- violation_code
    (select "OrdLaw" from dah_ordinance od where z."ViolDescID" = od."OrdID"),
    -- violation_description
    (select "OrdDescription" from dah_ordinance od where z."ViolDescID" = od."OrdID"),
    -- fine_amount
    (select "OffFineAmt" from dah_cityfines cf where z."OrigFineAmt" = cf."OffFinID"),
    "LateFee",
    "AdminFee",
    "JSA",
    "DiscAmt",
    -- remediation cost
    (select sum("ServiceCost") from dah_blight_ticket_svc_cost bts where z."ZTicketID" = bts."ZTicketID" and bts."ServiceType" = 6)
from dah_ztickets z where z."VoidTicket" = 0; -- order by random() limit 5000;

-- new fine amt
update dah_joined j set fine_amount = z."NewFineAmt" from dah_ztickets z
    where j.ticket_id = z."ZTicketID" and z."NewFineAmt" > 0;

-- join violator info
update dah_joined j set
    violator_name = concat_ws(' ', i."FirstName", i."LastName")
from dah_violator_info i where j.ticket_id = i."ZTicketID";

-- join violator address
update dah_joined j set
    mailing_address_str_number = a."StreetNumber",
    mailing_address_str_name = a."StreetName",
    city = a."City",
    state = (select "StateAbrev" from dah_state s where s."StateID" = a."StateID"),
    zip_code = a."ZipCode",
    non_us_str_code = a."NonUsCity",
    country = (select "CountryDesc" from dah_country c where c."CountryID"::text = a."CountryID"::text)
from dah_violator_address a where a."ViolatorID" = j.violator_id;

-- join dah payments
update dah_joined j set
	admin_fee = p."AdminFee",
	state_fee = p."StateFee"
from dah_payments p where j.ticket_id = p."ZticketID";

-- compute judgment amount
-- Sum of "OrigFineAmt", "AdminFee", "StateFee", "LateFee" and "RemediationCost" minus "DiscAmt"
update dah_joined j
	set judgment_amount = (
		COALESCE(fine_amount, 0) + 
		COALESCE(admin_fee, 0) + 
		COALESCE(state_fee, 0) + 
		COALESCE(late_fee, 0) + 
		COALESCE(clean_up_cost, 0) -
		COALESCE(discount_amount, 0));

-- compute payment amount
-- Sum all rows with "PaymentAmt" for a unique "ZTicketID"
update dah_joined j set
	payment_amount = (select sum("PaymentAmt") from dah_payments where "ZticketID" = j.ticket_id);
update dah_joined j set
    payment_amount = judgment_amount
        where j.ticket_id in (select "ZticketID" from dah_payments where "PymtKnd" = 2);

-- compute balance due
update dah_joined j
    set balance_due = (judgment_amount - COALESCE(payment_amount, 0));

-- compute payment date
update dah_joined j set
	payment_date = (select max("PaymentDt") from dah_payments where "ZticketID" = j.ticket_id);

-- create disposition string
update dah_joined j 
    set disposition = 
    (select concat_ws(' ',
        (select dt."Distype" from dah_disp_type dt where da."RespType" = dt."DispositionID"), 
        'by',
	    (select dt."Distype" from dah_disp_type dt where da."DispositionID" = dt."DispositionID")
        )
    )
	from dah_dispadjourn da where da."ZTicketID" = j.ticket_id;

-- if disposition is "not responsible", 
-- set payment and judgement to $0 (because might be refunded)
update dah_joined j set 
    payment_amount = 0, 
    judgment_amount = 0, 
    admin_fee = 0, 
    state_fee = 0,
    balance_due = 0
where disposition like 'Not resp%';

-- create payment status
update dah_joined j
    set payment_status = (
        CASE 
            WHEN 
            (j.disposition LIKE 'Not responsible%' OR
                j.disposition LIKE 'Responsible (Fine Waived)%') THEN 'NO PAYMENT DUE'
            WHEN j.payment_amount = 0 THEN 'NO PAYMENT APPLIED'
            WHEN j.payment_amount > 0 and j.balance_due > 0 THEN 'PARTIAL PAYMENT APPLIED'
            WHEN j.ticket_id in (select "ZticketID" from dah_payments where "PymtKnd" = 2) THEN 'PAID IN FULL'
            ELSE null
        END
    );

-- zero out balance due where no payment due
update dah_joined j set
    balance_due = 0 where j.payment_status = 'NO PAYMENT DUE';

-- create collection status
update dah_joined j
    set collection_status = 'In collections' where j.ticket_id in 
    (select "ZTicketID" from dah_dispadjourn where "CollectionFlag" != 'NaN');

-- create violation address
update dah_joined j
    set violation_address = concat_ws(' ', violation_street_number, violation_street_name);

-- join on parcel address
update dah_joined j set 
    parcelno = a.parcelno,
    propaddr = a.propaddr,
    legaldesc = a.legaldesc,
    latitude = ST_Y(ST_Transform(ST_Centroid(wkb_geometry), 4326)),
    longitude = ST_X(ST_Transform(ST_Centroid(wkb_geometry), 4326))
from base.joined a
    where trim(lower(j.violation_address)) = trim(lower(a.propaddr));