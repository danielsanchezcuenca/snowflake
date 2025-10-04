use database MY_DB;
create or replace stage CODE_STAGE;
grant usage on database MY_DB to role SYSADMIN;
grant usage on schema MY_DB.PUBLIC to role SYSADMIN;

grant read on stage MY_DB.PUBLIC.CODE_STAGE to role SYSADMIN; 
grant write on stage MY_DB.PUBLIC.CODE_STAGE to role SYSADMIN; 

--grant usage on stage MY_DB.PUBLIC.CODE_STAGE to role SYSADMIN;-- si hace falta

