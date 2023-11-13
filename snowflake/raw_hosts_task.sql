-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Use COMPUTE_WH warehouse
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS tasks;

USE tasks;

CREATE SCHEMA IF NOT EXISTS bronze;



CREATE OR REPLACE PROCEDURE CREATE_RAW_HOSTS_PROC ( )
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        var create_stmt = " CREATE OR REPLACE TABLE tasks.bronze.raw_hosts  (id integer, name string, is_superhost string, created_at datetime, updated_at datetime);"

        var stream_stmt = " CREATE OR REPLACE STREAM tasks.bronze.raw_hosts_stream on table tasks.bronze.raw_hosts;"

        var load_stmt = "COPY INTO tasks.bronze.raw_hosts (id, name, is_superhost, created_at, updated_at) from 's3://logbrain-datasets/airbnb/hosts.csv' FILE_FORMAT = (type = 'CSV' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ); "

        snowflake.execute( { sqlText: create_stmt });
        snowflake.execute( { sqlText: stream_stmt });
        snowflake.execute( { sqlText: load_stmt });

        return "Successfully executed.";
        $$;

SHOW PROCEDURES;

-- Create task with store prodedure
CREATE OR REPLACE TASK CREATE_RAW_HOSTS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS CALL  CREATE_RAW_HOSTS_PROC ();

SHOW TASKS;

--  Start  task
ALTER TASK CREATE_RAW_HOSTS_TASK RESUME;

SELECT * FROM tasks.bronze.raw_hosts;


-- Clean script


DROP STREAM tasks.bronze.raw_hosts_stream;

DROP PROCEDURE tasks.bronze.CREATE_RAW_HOSTS_PROC;

DROP TASK tasks.bronze.CREATE_RAW_HOSTS_TASK;

DROP TABLE tasks.bronze.raw_hosts;

DROP SCHEMA tasks.bronze;

DROP DATABASE tasks;