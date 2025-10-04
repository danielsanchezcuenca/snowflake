/*use database MY_DB;
use schema MY_DB.RAW;

LISTAR LOS JSONS DE GCS

DESC STAGE ST_RAW;
LIST @ST_RAW;*/

/* =========================================================
   0) Contexto
   ========================================================= */
--use role sysadmin;
--use warehouse compute_wh;
--use database my_db;
/* =========================================================
   1) Crear schema
   ========================================================= */

/* =========================================================
   2) Helper: función para extraer fecha del path (opcional)
   - Intenta leer 'dt=YYYY-MM-DD' o 'partition_date=YYYY-MM-DD'
   ========================================================= */

create or replace function BRONZE.PATH_TO_DATE(path string)
returns date
language sql
as
$$
coalesce(
    to_date(regexp_substr(path, 'partition_date=([0-9-]+)',1,1,'e',1)),
    to_date(regexp_substr(path, 'dt=([0-9-]+)', 1, 1, 'e', 1))
)
$$;


/* =========================================================
   3) ENTIDAD: BALANCE
   - Tabla BRONZE 'Pelada': JSON + TRAZABILIDAD
   ========================================================= */


create table if not exists BRONZE.BRONZE_BALANCE(

    raw variant,  --JSON crudo
    src_file string,  --Nombre del fichero
    src_dt date,     --fecha extraída del path
    load_ts timestamp default current_timestamp(),   --cuando lo cargaste
    raw_hash  string    -- hash del JSON

);
/* staging por ejecución para evitar duplicados exactos */

CREATE OR REPLACE TEMPORARY TABLE BRONZE.BRONZE_BALANCE_LOAD like BRONZE.BRONZE_BALANCE;

create or replace file format FF_JSON_ARRAY
  type = json;


/*COPY desde el stage a la tabla staging*/
  
copy into BRONZE.BRONZE_BALANCE_LOAD (raw, src_file, src_dt, load_ts, raw_hash)
from (
    select 
        $1,
        metadata$filename,
        BRONZE.PATH_TO_DATE(metadata$filename),
        current_timestamp(),
        md5(to_json($1))
    from @RAW.ST_RAW/balance/
)
file_format = (format_name = FF_JSON_ARRAY)  
on_error = 'continue';

--list @RAW.ST_RAW;

/*MERGE idempotente: evita insertar duplicados exactos por src_file y hash*/


merge into BRONZE.BRONZE_BALANCE t
using(
    select * from BRONZE_BALANCE_LOAD
) s

on s.src_file = t.src_file
and t.raw_hash = s.raw_hash
when not matched then insert (raw, src_file, src_dt, load_ts, raw_hash)
values (s.raw, s.src_file, s.src_dt, s.load_ts, s.raw_hash);

--LIMPIAR STAGING

truncate table BRONZE_BALANCE_LOAD;

/* COMPROBACIONES

select * from BRONZE_BALANCE;

select count(*) as fila, min(src_dt) as min_dt, max(src_dt) as max_dt from BRONZE_BALANCE;

select typeof(raw) as tipo_json, src_file from BRONZE_BALANCE;

FIN COMPROBACIONES*/

--INSERTS EN LAS TABLAS DE CONTROL, PARA NO REPETIR CARGAS EN DÍAS SIGUIENTES

--LIST @RAW.ST_RAW;


/* =========================================================
   4) ENTIDAD: ESTRUCTURA_GENERACION
   - Tabla BRONZE 'Pelada': JSON + TRAZABILIDAD
   ========================================================= */


create table if not exists BRONZE.BRONZE_ESTRUCTURA(

    raw variant, --JSON crudo
    src_file string,  --Nombre del fichero
    src_dt date,     --fecha extraída del path
    load_ts timestamp default current_timestamp(),   --cuando lo cargaste
    raw_hash  string    -- hash del JSON

);
/* staging por ejecución para evitar duplicados exactos */

CREATE OR REPLACE TEMPORARY TABLE BRONZE.BRONZE_ESTRUCTURA_LOAD like BRONZE.BRONZE_ESTRUCTURA;




/*COPY desde el stage a la tabla staging*/
  
copy into BRONZE.BRONZE_ESTRUCTURA_LOAD (raw, src_file, src_dt, load_ts, raw_hash)
from (
    select 
        $1,
        metadata$filename,
        BRONZE.PATH_TO_DATE(metadata$filename),
        current_timestamp(),
        md5(to_json($1))
    from @RAW.ST_RAW/estructura-generacion/
)
file_format = (format_name = FF_JSON_ARRAY)  
on_error = 'continue';

--list @RAW.ST_RAW;

/*MERGE idempotente: evita insertar duplicados exactos por src_file y hash*/


merge into BRONZE.BRONZE_ESTRUCTURA t
using(
    select * from BRONZE_ESTRUCTURA_LOAD
) s

on s.src_file = t.src_file
and t.raw_hash = s.raw_hash
when not matched then insert (raw, src_file, src_dt, load_ts, raw_hash)
values (s.raw, s.src_file, s.src_dt, s.load_ts, s.raw_hash);

--LIMPIAR STAGING

truncate table BRONZE_ESTRUCTURA_LOAD;

/* COMPROBACIONES

select * from BRONZE_ESTRUCTURA;

select count(*) as fila, min(src_dt) as min_dt, max(src_dt) as max_dt from BRONZE_ESTRUCTURA;

select typeof(raw) as tipo_json, src_file from BRONZE_ESTRUCTURA;

FIN COMPROBACIONES*/

--INSERTS EN LAS TABLAS DE CONTROL, PARA NO REPETIR CARGAS EN DÍAS SIGUIENTES

--LIST @RAW.ST_RAW;


/* =========================================================
   5) ENTIDAD: IRE_GENERAL
   - Tabla BRONZE 'Pelada': JSON + TRAZABILIDAD
   ========================================================= */


   create table if not exists BRONZE.BRONZE_IRE(

    raw variant,  --JSON crudo
    src_file string,  --Nombre del fichero
    src_dt date,     --fecha extraída del path
    load_ts timestamp default current_timestamp(),   --cuando lo cargaste
    raw_hash  string    -- hash del JSON

);
/* staging por ejecución para evitar duplicados exactos */

CREATE OR REPLACE TEMPORARY TABLE BRONZE.BRONZE_IRE_LOAD like BRONZE.BRONZE_IRE;




/*COPY desde el stage a la tabla staging*/
  
copy into BRONZE.BRONZE_IRE_LOAD (raw, src_file, src_dt, load_ts, raw_hash)
from (
    select 
        $1,
        metadata$filename,
        BRONZE.PATH_TO_DATE(metadata$filename),
        current_timestamp(),
        md5(to_json($1))
    from @RAW.ST_RAW/ire_general/
)
file_format = (format_name = FF_JSON_ARRAY)  
on_error = 'continue';

--list @RAW.ST_RAW;

/*MERGE idempotente: evita insertar duplicados exactos por src_file y hash*/


merge into BRONZE.BRONZE_IRE t
using(
    select * from BRONZE_IRE_LOAD
) s

on s.src_file = t.src_file
and t.raw_hash = s.raw_hash
when not matched then insert (raw, src_file, src_dt, load_ts, raw_hash)
values (s.raw, s.src_file, s.src_dt, s.load_ts, s.raw_hash);

--LIMPIAR STAGING

truncate table BRONZE_IRE_LOAD;

/* COMPROBACIONES

select * from BRONZE_IRE;

select count(*) as fila, min(src_dt) as min_dt, max(src_dt) as max_dt from BRONZE_IRE;

select typeof(raw) as tipo_json, src_file from BRONZE_IRE;

FIN COMPROBACIONES*/

--INSERTS EN LAS TABLAS DE CONTROL, PARA NO REPETIR CARGAS EN DÍAS SIGUIENTES

--LIST @RAW.ST_RAW;


