create role analytics_reader;
create role data_quality_operator;

grant select on schema::gold to analytics_reader;
grant select on schema::mart to analytics_reader;
grant select on schema::gold to data_quality_operator;
grant select on schema::stg to data_quality_operator;

-- Membership is environment-specific and intentionally not versioned here:
-- alter role analytics_reader add member [<entra-principal>];

