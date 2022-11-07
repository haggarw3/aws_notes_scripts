create schema if not exists "core-profile-entities-temp";


-- drop tables from the "core-profile-entities-temp" schema so that latest data from "core-profile-entities" schema can be captured there 
drop table if exists "core-profile-entities-temp"."location";
drop table if exists "core-profile-entities-temp"."tire-profile";
drop table if exists "core-profile-entities-temp"."contacts";


-- create tables in "core-profile-entities-temp" schema like "core-profile-entities" schema 
create table if not exists "core-profile-entities-temp"."location" (like "core-profile-entities"."location");
create table if not exists "core-profile-entities-temp"."tire-profile" (like "core-profile-entities"."tire-profile");
create table if not exists "core-profile-entities-temp"."contacts" (like "core-profile-entities"."contacts");



-- location table 

insert into "core-profile-entities-temp"."location"
select * from "core-profile-entities"."location";

delete from "core-profile-entities"."location";

-- tire-profile table 
insert into "core-profile-entities-temp"."tire-profile"
select * from "core-profile-entities"."tire-profile";

delete from "core-profile-entities"."tire-profile";


-- contacts table 

insert into "core-profile-entities-temp"."contacts"
select * from "core-profile-entities"."contacts";

delete from "core-profile-entities"."contacts";