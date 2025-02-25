create database inc_db;
use database inc_db;
create or replace schema raw_layer;
create schema dev_layer;

create schema prod_layer;
use schema raw_layer;
create or replace file format csv_format
type = csv
field_delimiter = ','
skip_header = 1
date_format = 'DD-MM-YYYY'
null_if = ('null','null')
empty_field_as_null = true
field_optionally_enclosed_by = '\042'
compression = auto;


create  or replace stage my_stage
file_format = csv_format;

create or replace table customer(
    index int,
    customer_id varchar ,
    first_name varchar,
    last_name varchar,
    company varchar,
    city varchar,
    country varchar,
    phone_1 varchar,
    phone_2 varchar,
    email varchar,
    subscription_date date,
    web_site varchar,
    last_updated_at timestamp
);

copy into raw_layer.customer from (
    select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,METADATA$FILE_LAST_MODIFIED from @raw_layer.my_stage
)
on_error = continue
purge = true;
select * from raw_layer.customer;

select * from INFORMATION_SCHEMA.LOAD_HISTORY
WHERE TABLE_NAME = 'CUSTOMER';

select count(*) from  dev_layer.dim_customer
where country = 'India';
select * from  prod_layer.summarized_table;

create or replace table inc_db.dev_layer.dim_customer(
    index int,
    customer_id varchar ,
    first_name varchar,
    last_name varchar,
    company varchar,
    city varchar,
    country varchar,
    phone_1 varchar,
    phone_2 varchar,
    email varchar,
    subscription_date date,
    web_site varchar,
    last_updated_at timestamp
);
use database inc_db;
select * from dev_layer.dim_customer;
select * from prod_layer.summarized_table;

select customer_id ,Count(*) from dev_layer.dim_customer
group by customer_id
having count(*) = 1;

with cte as (
    select row_number() over (partition by customer_id order by last_updated_at desc)as rn from dev_layer.dim_customer
)
select * from cte 
where rn > 1;