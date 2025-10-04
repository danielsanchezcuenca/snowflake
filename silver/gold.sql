
alter table estructura_atributos add column total number(18,3);
alter table ire_atributos add column total number(18,3);

merge into estructura_atributos i
using(
    select a.clave_estructura, sum(v.valor) as total 
    from estructura_atributos a
    left join estructura_valor v on v.clave_estructura=a.clave_estructura
    group by a.clave_estructura
) s
on s.clave_estructura = i.clave_estructura
when matched then update set
    i.total = s.total;



merge into ire_atributos i
using(
    select  
     a.clave_ire,sum(v.valor) as total
    from ire_atributos a
    left join ire_valor v on v.clave_ire=a.clave_ire
    group by a.clave_ire
) s
on s.clave_ire = i.clave_ire
when matched then update set
    i.total= s.total;

