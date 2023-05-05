create database if not exists ${DB} location '${LOCATIONPATH}/${DB}.db';
use ${DB};

drop table if exists catalog_page;

create table catalog_page
stored as ${FILE}
as select * from ${SOURCE}.catalog_page;
