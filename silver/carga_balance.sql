/*use role SYSADMIN;
use database MY_DB;

create schema if not exists SILVER;
use schema SILVER;*/

-- Metadatos de la cabecera
--select raw from BRONZE.BRONZE_BALANCE;

/*
---------------------------- BALANCE ------------------------------
---Usamos los datos de dentro del included y comenzamos a acceder a las diferentes etiquetas usando flatten
---------------------------- BALANCE ------------------------------
*/


create or replace view BALANCE_ALL as
select 

    b.src_dt,
    b.src_file,
    inc.value::variant as included ,
    cont.value::variant as content_item 
    
from BRONZE.BRONZE_BALANCE b,
    lateral flatten(input => b.raw:"data":"included") inc,
    lateral flatten (input => inc.value:"attributes":"content", outer => true) cont;


--select * from balance_all

-- CREAMOS LA TABLA POR BALANCE SIN VALUES, LA CLAVE ÚNICA ES id_tipo + last_update


create or replace view BALANCE_TECH as 
select 
    concat(content_item:"id"::string,'-',content_item:"attributes":"last-update"::string) as clave_tech,
    included:"id"::string as id_energy,
    included:"type"::string as type_energy,
    content_item:"id"::string as id_tipo,
    content_item:"type"::string as nombre_tipo,
    content_item:"groupId"::string as grupo_tipo,
    content_item:"attributes":"color"::string as color,
    content_item:"attributes":"composite"::boolean as composite,
    content_item:"attributes":"last-update"::string as last_update,
    content_item:"attributes":"magnitude"::string as magnitude,
    content_item:"attributes":"total"::number(18,3) as total,
    content_item:"attributes":"total-percentage"::number(18,3) as porcentaje_total
    
from BALANCE_ALL b
where included is not null and content_item is not null;



/*          COMPROBACIONES

            select count(*) as contador, id_tipo,last_update
            from balance_tech group by id_tipo,last_update
            
            
            
            select * from balance_tech where id_tipo='No-Renovable'


*/


/*VISTA DE VALORES POR DE LOS DIFERENTES REGISTROS DE TECH POR MES,
    LA CLAVE PRIMARIA ES CLAVE_TECH + FECHA_VALOR
*/

create or replace view BALANCE_VALUES as
select 
    concat(content_item:"id"::string,'-',content_item:"attributes":"last-update"::string,'-',val.value:"datetime"::string) as clave_valor,
    concat(content_item:"id"::string,'-',content_item:"attributes":"last-update"::string) as clave_tech,
    val.value:"value"::number(18,3) as value,
    try_to_date(val.value:"datetime"::string) as fecha_valor,
    val.value:"percentage"::number(18,3) as porcentaje_valor
from balance_all b,
    lateral flatten(input => b.content_item:"attributes":"values") val
where b.included is not null and b.content_item is not null;


/*COMPROBACIONES
select count(*) as contador from balance_values  group by clave_tech,fecha_valor having contador > 1
*/


/*

COMO ÚLTIMO PASO NOS FALTA EL MERGE EN LAS TABLAS QUE VAN A ESTAR ESTABLES.
REALIZAMOS UNA MERGE INTO CON UN INSERT IF NOT MATCHED.
select * from balance_values
*/

/*CREAMOS TABLAS*/
 
create table if not exists balance_tech_t(

        clave_tech string,
        id_energy string,
        type_energy string,
        id_tipo string,
        nombre_tipo string,
        grupo_tipo string,
        color string,
        composite string,
        last_update string,
        magnitude string,
        total number(18,3),
        porcentaje_total number(18,3)
);

merge into balance_tech_t t
using (
  select * from balance_tech
) b
on t.clave_tech = b.clave_tech
when matched then update set 
                clave_tech      = b.clave_tech,
                id_energy       = b.id_energy,
                type_energy     = b.type_energy,
                id_tipo         = b.id_tipo,
                nombre_tipo     = b.nombre_tipo,
                grupo_tipo      = b.grupo_tipo,
                color           = b.color,
                composite       = b.composite,
                last_update     = b.last_update,
                magnitude       = b.magnitude,
                total           = b.total,
                porcentaje_total= b.porcentaje_total
                
when not matched then insert
  (clave_tech, id_energy, type_energy, id_tipo, nombre_tipo, grupo_tipo, color, composite, last_update, magnitude, total, porcentaje_total)
values
  (b.clave_tech, b.id_energy, b.type_energy, b.id_tipo, b.nombre_tipo, b.grupo_tipo, b.color, b.composite, b.last_update, b.magnitude, b.total, b.porcentaje_total);


/*COMPROBACIONES select * from balance_tech_t*/

/*REPETIMOS LA ACCIÓN PARA LA TABLA BALANCE_VALUES*/

create table if not exists BALANCE_VALUES_T(
        clave_valor string,
        clave_tech string,
        value number(18,3),
        fecha_valor datetime,
        porcentaje_valor number(18,3)
);


merge into BALANCE_VALUES_T t
using (
        select * from BALANCE_VALUES
) b

on t.clave_valor = b.clave_valor
when matched then update set
                clave_valor      = b.clave_valor,
                clave_tech       = b.clave_tech,
                value            = b.value,
                fecha_valor      = b.fecha_valor,
                porcentaje_valor = b.porcentaje_valor
when not matched then insert(clave_valor, clave_tech, value, fecha_valor, porcentaje_valor) values (b.clave_valor, b.clave_tech, b.value, b.fecha_valor, b.porcentaje_valor);

/*FINALIZAMOS LA CARGA DE BALANCE*/









