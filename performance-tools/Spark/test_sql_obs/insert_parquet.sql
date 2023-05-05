create database if not exists tpcds_parquetdb location 'obs://800mrs/tmp/tpcds_parquetdb';
use tpcds_parquetdb;

create table catalog_sales using parquet as select * from tpcds_hivedb.catalog_sales;
create table store_sales using parquet as select * from tpcds_hivedb.store_sales;
create table web_sales using parquet as select * from tpcds_hivedb.web_sales;