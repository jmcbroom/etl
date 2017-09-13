update assessor.baseattr b 
    set taxpayer1 = u.ownername1,
        taxpayer2 = u.ownername2,
        taxpaddr = u.ownerstreetaddr,
        taxpcity = u.ownercity,
        taxpstate = u.ownerstate,
        taxpzip = u.ownerzip,
        -- propclass
        -- prevpclass
        -- taxstatus
        -- prevtstatus
        -- zoning
        saleprice = u."lastSalePrice",
        saledate = u."lastSaleDate"
        -- landav
        -- bldgav
        -- av
        -- prevav
        -- tv
        -- prevtv
        -- sev
        -- landvalue
    from assessor.updt u
    where b.parcelno = u.pnum;

drop table if exists assessor.joined cascade;
create table assessor.joined as (select s.parcelno as pnum, s.wkb_geometry, b.* from assessor.shapes s inner join assessor.baseattr b on b.parcelno = s.parcelno);
