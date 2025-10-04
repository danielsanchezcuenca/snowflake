use database MY_DB;
create schema if not exists CTRL;
use schema CTRL;



--TABLA DE REGISTRO DE ERRORES:
create table if not exists logs(
    ts      datetime default current_timestamp(),
    procedimiento string,
    mensaje       string
);


