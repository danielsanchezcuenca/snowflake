--use role SYSADMIN;
--use database MY_DB;

--create schema if not exists SILVER;
--use schema SILVER;

--SELECT RAW FROM BRONZE.BRONZE_IRE;

create or replace view IRE_ALL as
select 

    b.src_dt,
    b.src_file,
    inc.value::variant as included ,
    --cont.value::variant as content_item 
    
from BRONZE.BRONZE_IRE b,
    lateral flatten(input => b.raw:"data":"included") inc;


--    select COUNT(*) from ire_all


/*YA TENEMOS TODO LO QUE NECESITAMOS EN IRE_ALL Vamos a PROBAR a hacer un solo merge idempotente sin vistas ni tablas staging*/

create or replace table IRE_atributos(
    clave_ire string,
    id string,
    tipo string,
    groupID string,
    color string,
    composite string,
    description string,
    last_update string,
    magnitude string,
    titulo string,
    subtipo string

);


merge into IRE_atributos i
using(
    select 
        concat(included:"id"::string,'-',included:"attributes":"last-update"::string) as clave_ire,
        included:"id"::string as                            id,
        included:"type"::string as                          tipo,
        included:"groupId"::string as                       groupID,
        included:"attributes":"color"::string as            color,
        included:"attributes":"composite"::string as        composite,
        included:"attributes":"description"::string as      description,
        included:"attributes":"last-update"::datetime as    last_update,
        included:"attributes":"magnitude"::string as        magnitude,
        included:"attributes":"title"::string as            titulo,
        included:"attributes":"type"::string as             subtipo
    from ire_all a
    where included is not null
) s

on s.clave_ire = i.clave_ire
when matched then update set 
    clave_ire   =s.clave_ire,
    id          =s.id,
    tipo        =s.tipo,
    groupID     =s.groupID,
    color       =s.color,
    composite   =s.composite,
    description =s.description,
    last_update =s.last_update,
    magnitude   =s.magnitude,
    titulo      =s.titulo,
    subtipo     =s.subtipo
when not matched then insert(clave_ire,id,tipo,groupID,color,composite,description,last_update,magnitude,titulo,subtipo)
values(s.clave_ire,s.id,s.tipo,s.groupID,s.color,s.composite,s.description,s.last_update,s.magnitude,s.titulo,s.subtipo);

/*AHORA DEBEMOS INSERTAR LOS VALUES*/





/*CREACIÓN DE LA TABLA*/

create or replace table IRE_valor(
    clave_ire string,
    clave_valor string,
    fecha_valor datetime,
    porcentaje  number(18,3),
    valor number(18,3)

);

merge into IRE_valor i
using(

    select 
        concat(included:"id"::string,'-',included:"attributes":"last-update"::string) as clave_ire,
        concat(included:"id"::string,'-',included:"attributes":"last-update"::string,'-', val.value:"datetime"::string) as clave_valor,
        val.value:"datetime"::datetime as fecha_valor,
        try_to_number(val.value:"percentage"::string, 18,3) as porcentaje, 
        try_to_number(val.value:"value"::string, 18,3) as valor
    from ire_all a,
        lateral flatten(input => a.included:"attributes":"values") val
    where a.included is not null

) s
on s.clave_valor = i.clave_valor

when matched then update set
    clave_ire = s.clave_ire,
    clave_valor = s.clave_valor,
    fecha_valor = s.fecha_valor,
    porcentaje = s.porcentaje,
    valor = s.valor
when not matched then insert (clave_ire, clave_valor,fecha_valor,porcentaje,valor)
values(s.clave_ire, s.clave_valor,s.fecha_valor,s.porcentaje,s.valor);


/*
select a.tipo, avg(v.valor) as media_total, min(v.fecha_valor), max(v.fecha_valor)
from IRE_VALOR v
left join ire_atributos a on a.clave_ire=v.clave_ire 
group by a.tipo;
*/



    


