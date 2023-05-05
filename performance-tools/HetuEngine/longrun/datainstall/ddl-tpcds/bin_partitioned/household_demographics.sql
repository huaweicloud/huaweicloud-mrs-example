create database if not exists ${DB} location '${LOCATIONPATH}/${DB}.db';
use ${DB};

drop table if exists household_demographics;

create table household_demographics
stored as ${FILE}
as select * from ${SOURCE}.household_demographics;
