--use role SYSADMIN;
----use database MY_DB;

--create schema if not exists SILVER;
--use schema SILVER;

SELECT RAW FROM BRONZE.BRONZE_ESTRUCTURA;


create or replace view ESTRUCTURA_ALL as
select 

    b.src_dt,
    b.src_file,
    inc.value::variant as included 
    --cont.value::variant as content_item 
    
from BRONZE.BRONZE_ESTRUCTURA b,
    lateral flatten(input => b.raw:"data":"included") inc;


--SELECT * FROM ESTRUCTURA_ALL LIMIT 4;



create or replace table ESTRUCTURA_ATRIBUTOS(
    clave_estructura string,
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



merge into ESTRUCTURA_ATRIBUTOS e
using(

    select 
        concat(included:"id"::string,'-',included:"attributes":"last-update"::string) as clave_estructura,
        included:"id"::string as                    id,
        included:"type"::string as                  tipo,
        included:"groupId"::string as               groupID,
        included:"attributes":"color"::string        color,
        included:"attributes":"composite"::string    composite,
        included:"attributes":"description"::string  description,
        included:"attributes":"last-update"::string   last_update,
        included:"attributes":"magnitude"::string     magnitude,
        included:"attributes":"title"::string        titulo,
        included:"attributes":"type"::string        subtipo
        
    from estructura_all
    where included is not null

) s

on e.clave_estructura = s.clave_estructura
when matched then update set
            clave_estructura=       s.clave_estructura,
            id              =       s.id,
            tipo            =       s.tipo,
            groupID         =       s.groupID,
            color           =       s.color,
            composite       =       s.composite,
            description     =       s.description,
            last_update     =       s.last_update,
            magnitude       =       s.magnitude,
            titulo          =       s.titulo,
            subtipo         =       s.subtipo

when not matched then insert(
                        clave_estructura,
                        id         ,
                        tipo       ,
                        groupID    ,
                        color      ,
                        composite  ,
                        description,
                        last_update,
                        magnitude  ,
                        titulo     ,
                        subtipo    
)

values(
                        s.clave_estructura,
                        s.id         ,
                        s.tipo       ,
                        s.groupID    ,
                        s.color      ,
                        s.composite  ,
                        s.description,
                        s.last_update,
                        s.magnitude  ,
                        s.titulo     ,
                        s.subtipo    
);




--select count(*) as contador,clave_estructura from estructura_atributos group by clave_estructura


create or replace table ESTRUCTURA_VALOR(
    clave_estructura string,
    clave_valor string,
    fecha_valor datetime,
    porcentaje  number(18,3),
    valor number(18,3)

);

merge into  ESTRUCTURA_VALOR i
using(

    select 
        concat(included:"id"::string,'-',included:"attributes":"last-update"::string) as clave_estructura,
        concat(included:"id"::string,'-',included:"attributes":"last-update"::string,'-', val.value:"datetime"::string) as clave_valor,
        val.value:"datetime"::datetime as fecha_valor,
        try_to_number(val.value:"percentage"::string, 18,3) as porcentaje, 
        try_to_number(val.value:"value"::string, 18,3) as valor
    from estructura_all a,
        lateral flatten(input => a.included:"attributes":"values") val
    where a.included is not null

) s
on s.clave_valor = i.clave_valor

when matched then update set
    clave_estructura = s.clave_estructura,
    clave_valor = s.clave_valor,
    fecha_valor = s.fecha_valor,
    porcentaje = s.porcentaje,
    valor = s.valor
when not matched then insert (clave_estructura, clave_valor,fecha_valor,porcentaje,valor)
values(s.clave_estructura, s.clave_valor,s.fecha_valor,s.porcentaje,s.valor);


select * from estructura_valor limit 5;







