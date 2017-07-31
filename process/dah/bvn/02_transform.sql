
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
    mailing_address_str_number text,
    mailing_address_str_name text,
    city text,
    state text,
    zip_code text,
    non_us_str_code text,
    country text,
    ticket_issued_date timestamp,
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
    grafitti_status text,
    violation_address text,
    parcelno text,
    propaddr text,
    legaldesc text,
    latitude double precision,
    longitude double precision,
    location text);

-- create indices on helper tables. speeds up ze joins
drop index if exists dah_joined_zticket_idx;
create index dah_joined_zticket_idx on dah_joined using btree(ticket_id);

drop index if exists dah_violator_info_idx;
create index dah_violator_info_idx on dah_violator_info using btree("ZTicketID");

drop index if exists dah_violator_address_idx;
create index dah_violator_address_idx on dah_violator_address using btree("ViolatorID");

drop index if exists dah_payment_id_idx;
create index dah_payment_id_idx on dah_payments using btree("ZticketID");

insert into dah_joined (
    ticket_id,
    ticket_number,
    agency_name,
    inspector_name,
    violation_street_number,
    violation_street_name,
    violation_zip_code,
    ticket_issued_date,
    ticket_issued_time,
    hearing_date,
    hearing_time,
    violation_code,
    fine_amount,
    late_fee,
    discount_amount,
    clean_up_cost
)
select
    "ZTicketID",
    "TicketNumber",
    "AgencyID",
    "ZTicketUserID",
    "ViolStreetNumber",
    "ViolStreetName",
    "ViolZipCode",
    "IssueDate"::timestamp,
    "IssueTime",
    "CourtDate",
    "CourtTime",
    "ViolDescID",
    (select "OrigFineAmt",
    "LateFee",
    "DiscAmt",
    "RemediationCost"
from dah_ztickets z where z."VoidTicket" = 0;

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
    country = (select "CountryDesc" from dah_country c where c."CountryID" = a."CountryID")
from dah_violator_address a where a."ViolatorID" = j.ticket_id;

-- join dah payments
update dah_joined j set
	admin_fee = p."AdminFee",
	state_fee = p."StateFee"
from dah_payments p where j.ticket_id = p."ZticketID";

-- compute payment amount
-- Sum all rows with "PaymentAmt" for a unique "ZTicketID"
update dah_joined j set
	payment_amount = (select sum("PaymentAmt") from dah_payments where "ZticketID" = j.ticket_id);

-- lookup original fine amount
update dah_joined j set
	fine_amount = (select "OffFineAmt" from dah_cityfines cf where j.fine_amount = cf."OffFinID");


-- -- PICK UP HERE!

-- compute judgment amount
-- Sum of "OrigFineAmt", "AdminFee", "StateFee", "LateFee" and "RemediationCost" minus "DiscAmt"
update dah_joined j
	set judgment_amount = (
		fine_amount + 
		admin_fee +
		state_fee +
		late_fee +
		remediation_cost -
		discount_amount);
	
select * from dah_joined j order by random() limit 100;