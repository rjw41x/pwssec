-- need to start a gpfdist process and point it to the directory where the files live
create schema ext;
/* groups
"id","displayName","created","lastModified","version","identity_zone_id","description"
"0038d408-c265-4281-a53b-495ccc962a62","cloud_controller_service_permissions.read","2015-08-17 19:32:20","2015-08-17 19:32:20",0,"b7ebd246-18db-4f17-9a56-c7f28b4f1a4d",NULL
*/
drop table if exists groups ;
create table groups (
    id  varchar
    , displayName varchar
    , create_ts timestamp
    , last_mod_ts timestamp
    , version int
    , identity_zone_id varchar
    , description varchar
) distributed by ( id );

drop external table ext.groups;
create readable external table ext.groups ( like public.groups )
location ('gpfdist://mdw:8081/all_groups_20161115.csv')
format 'CSV' ( delimiter ',' quote '"' null '' HEADER );

/*
users
"username","id","email","displayName","identity_zone_id"
"sgu@pivotal.io","0112a550-5272-4f01-a9b0-8b4e04703bf3","sgu@pivotal.io","console.support","uaa"
"id","username","email","givenName","familyName","origin","identity_zone_id"
*/
drop table if exists users;
create table users (
    username varchar
    , id varchar
    , email varchar
    , displayname varchar
    , identity_zone_id varchar
) distributed by ( id );

drop external table if exists ext.users;
create readable external table ext.users ( 
    username varchar
    , id varchar
    , email varchar
    , displayname varchar
    , displayname2 varchar
    , origin varchar
    , identity_zone_id varchar
)
location ('gpfdist://mdw:8081/all_users_20161117.csv')
format 'CSV' ( delimiter ',' quote '"' null '' HEADER )
log errors into user_errors segment reject limit 1000;

insert into users select username, id, email, displayname || displayname2, identity_zone_id from ext.users;
/*
clients
"client_id","authorized_grant_types","identity_zone_id","authorities","scope"
"002aa8a6-bd91-4a63-9ef5-3c83fad416b3","authorization_code","a10fc95e-ba23-46b8-9155-55c1fd704a63","uaa.resource","openid"
*/
drop table if exists clients;
create table clients (
    client_id varchar
    , auth_grant_types varchar
    , identity_zone_id varchar
    , aurthorities varchar
    , scope varchar
) distributed by ( client_id );

drop external table if exists ext.clients;
create readable external table ext.clients ( like public.clients )
location ('gpfdist://mdw:8081/all_clients_20161115.csv' )
format 'CSV' ( delimiter ',' quote '"' null '' HEADER );

/*
zones
"id"u"subdomain"u"name"u"config"
"016c4f8c-27d8-4b59-89e2-28b5866555c8"u"wtran-springone"u"SpringOne 2GX"uNULL
"id","subdomain","name"
"016c4f8c-27d8-4b59-89e2-28b5866555c8","wtran-springone","SpringOne 2GX"
*/
drop table if exists zones;
create table zones (
    id varchar
    , subdomain varchar
    , name varchar
) distributed by ( id );

drop external table if exists  ext.zones;
create readable external table ext.zones ( like public.zones )
location ('gpfdist://mdw:8081/all_zones_20161117.csv')
format 'CSV' ( delimiter ',' quote '"' null '' );
