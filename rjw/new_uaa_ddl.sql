CREATE SEQUENCE uaa_parsed_log_id_seq 
cache 1000;

drop table if exists new_uaa;
create table new_uaa (
log_id integer default nextval('uaa_parsed_log_id_seq'),
at_raw text,
tags varchar,
timer varchar,
log_ts timestamp ,
idx int,
src_ip inet,
job varchar,
vm varchar,
host varchar,
program varchar,
deployment varchar,
region_name varchar,
latitude varchar,
geo_ip inet,
area_code varchar,
continent_code varchar,
country_code3 varchar,
country_code2 varchar,
city_name varchar,
longitude varchar,
timezone varchar,
country_name varchar,
postal_code varchar,
real_region_name varchar,
dma_code varchar,
location varchar,
origin varchar,
thread_name varchar,
entry_type varchar,
pid int,
remote_address inet,
identity_zone_id varchar,
data varchar,
principal varchar,
raw_text text,
batch_id date )
with ( appendonly=true, compresstype=zlib, compresslevel=5 )
distributed by ( log_id )
partition by range ( batch_id )
(
PARTITION Jul16 START (date '2016-07-01') INCLUSIVE,
PARTITION Aug16 START (date '2016-08-01') INCLUSIVE,
PARTITION Sep16 START (date '2016-09-01') INCLUSIVE,
PARTITION Oct16 START (date '2016-10-01') INCLUSIVE,
PARTITION Nov16 START (date '2016-11-01') INCLUSIVE,
PARTITION Dec16 START (date '2016-12-01') INCLUSIVE
    END (date '2017-01-01') EXCLUSIVE
)
;
-- RJW - syntax for adding partitions
-- alter table netflow add partition Jan17 START ( date '01-01-2017' ) INCLUSIVE END ( date '02-01-2017' ) EXCLUSIVE ;
-- RJW - syntax for dropping partitions
-- alter table netflow drop partition Jan17;
/*
drop external table if exists ext_new_uaa;
create external table ext_new_uaa (
at_raw text,
log_ts timestamp ,
idx int,
src_ip inet,
job varchar,
vm varchar,
host varchar,
program varchar,
deployment varchar,
region_name varchar,
latitude varchar,
geo_ip inet,
area_code varchar,
continent_code varchar,
country_code3 varchar,
country_code2 varchar,
city_name varchar,
longitude varchar,
timezone varchar,
country_name varchar,
postal_code varchar,
real_region_name varchar,
dma_code varchar,
location varchar,
origin varchar,
thread_name varchar,
entry_type varchar,
pid int,
remote_address inet,
identity_zone_id varchar,
data varchar,
principal varchar,
raw_text text)
location ('gpfdist://mdw:8081/load_data')
format 'text' ( delimiter '|' null '' )
log errors into err_new_uaa segment reject limit 1000;
*/
-- RJW - insert statement validated to work against above tables
-- insert into new_uaa ( at_raw, log_ts, idx, src_ip, job, vm, host, program, deployment, region_name, latitude, geo_ip,
-- area_code, continent_code, country_code3, country_code2, city_name, longitude, timezone, country_name, postal_code,
-- real_region_name, dma_code, location, origin, thread_name, entry_type, pid, remote_address, identity_zone_id, data,
-- principal, raw_text, batch_id ) select *, '$DATE_STR' from ext_new_uaa;
