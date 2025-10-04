use role SYSADMIN;
use warehouse COMPUTE_WH;

create if not exists database MY_DB;
create if not exists schema MY_DB.RAW;

use database MY_DB;
use schema MY_DB.RAW;

USE ROLE ACCOUNTADMIN;

create or replace storage integration GCS_INT
    type = external_stage
    storage_provider = gcs
    enabled = true
    storage_allowed_locations=('gcs://redelectrica-raw/raw');
;


--desc storage integration GCS_INT;
grant usage on integration GCS_INT to role SYSADMIN;



create or replace file format FF_JSON_ARRAY
  type = json;

create or replace stage ST_RAW
    url = 'gcs://redelectrica-raw/raw'
    storage_integration = GCS_INT
    file_format = FF_JSON_ARRAY;
;



--LIST @ST_RAW;










