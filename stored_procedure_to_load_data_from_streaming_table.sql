use role accountadmin;
create or replace database my_db;
use database my_db;
create schema my_db.my_schema;
alter database my_db set DATA_RETENTION_TIME_IN_DAYS = 30;

create or replace table my_db.my_schema.stage_table(
    raw_data variant
);

create file format json_format
type =json;
create or replace stage my_stage
file_format = json_format;

copy into stage_table from @my_stage
purge = true;

truncate table info;

create or replace table info(
    name varchar,
    age int,
    email varchar,
    phone varchar,
    title varchar,
    last_updated_at timestamp
);

CREATE OR REPLACE PROCEDURE stored_proc_extract_json()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
DECLARE 
rows_before INT;
rows_after INT;
rows_inserted INT;

BEGIN
    -- Count rows before insertion
    SELECT COUNT(*) INTO rows_before FROM info;

    -- Insert new data into the info table from the stream
    INSERT INTO info
    SELECT  
        t.value:name::VARCHAR AS name,
        t.value:info.age::INT AS age,
        t.value:info.email::VARCHAR AS email,
        t.value:info.phone::VARCHAR AS phone,
        t.value:info.title::VARCHAR AS title,
        CURRENT_TIMESTAMP AS last_updated_at
    FROM my_stream,
         LATERAL FLATTEN(input => my_stream.raw_data) t
    WHERE metadata$action = 'INSERT';

    -- Count rows after insertion
    SELECT COUNT(*) INTO rows_after FROM info;

    -- Calculate inserted rows
    rows_inserted := rows_after - rows_before;

    -- Return success message with row count
     RETURN 'Rows_before: ' || rows_before || 
           ', Rows inserted: ' || rows_inserted || 
           ', Rows_after: ' || rows_after;
EXCEPTION 
    WHEN OTHER THEN 
        RETURN 'Failed: ' || SQLERRM;
END;
$$;



call stored_proc_extract_json();


create or replace stream my_db.my_schema.my_stream on table stage_table;

select * from info;
