use role SYSADMIN;
use database MY_DB;



use schema silver;

CREATE OR REPLACE PROCEDURE silver.SP_VALIDACION_2()
RETURNS STRING  -- o otro tipo si aplica
LANGUAGE SQL    -- puede ser SQL o JAVASCRIPT
AS
$$
DECLARE
    -- variables locales
    v_balance_suma STRING;
    v_balance_values STRING;
    v_balance_tech STRING;
    v_estructura_atributos STRING;
    v_estructura_values STRING;
    v_ire_atributos STRING;
    v_ire_valor STRING;
    EXCEPTION_1 EXCEPTION (-20001, 'Error en la validación de la suma de balance');
    EXCEPTION_2 EXCEPTION (-20001, 'Error en la validación de clave_valor en balance_values');
    EXCEPTION_3 EXCEPTION (-20001, 'Error en la validación de clave_tech en balance_tech');
    EXCEPTION_4 EXCEPTION (-20001, 'Error en la validación de clave_estructura en estructura_atributos');
    EXCEPTION_5 EXCEPTION (-20001, 'Error en la validación de clave_values en estructura_valor');
    EXCEPTION_6 EXCEPTION (-20001, 'Error en la validación de clave_ire en ire_atributos');
    EXCEPTION_7 EXCEPTION (-20001, 'Error en la validación de clave_valor en ire_valor');
BEGIN
    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN SUMA DE BALANCE
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    with agg as (
            select clave_tech, sum(value) as valor_mes_sum
            from silver.balance_values_t
            group by clave_tech
        )
        select TOP 1 t.clave_tech INTO: v_balance_suma
        from silver.balance_tech_t t
        left join agg a on a.clave_tech= t.clave_tech
        where trunc(a.valor_mes_sum,0) <> trunc(t.total,0);
        --where a.valor_mes_sum <> t.total;
    
    if (v_balance_suma is not null) THEN
        RAISE EXCEPTION_1;
    end if;
    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN ID BALANCE_VALUES EN CLAVE_VALOR 
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    select TOP 1 clave_valor into: v_balance_values
    from silver.balance_values_t
    group by clave_valor
    having count(*) > 1;

    if(v_balance_values is not null) THEN
        RAISE EXCEPTION_2;
    end if;

    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN ID BALANCE_TECH EN CLAVE_TECH
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    select top 1 clave_tech into:v_balance_tech
    from silver.balance_tech_t
    group by clave_tech
    having  count(*) > 1;

    if(v_balance_tech is not null) THEN
        RAISE EXCEPTION_3;
    end if;

     /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN ID ESTRUCTURA_ATRIBUTOS EN CLAVE_ESTRUCTURA
    ------------------------------------------------------------------------------------------------------------------------------------
    */

    select top 1 clave_estructura into: v_estructura_atributos
    from silver.estructura_atributos
    group by clave_estructura
    having count(*) > 1; 

    if(v_estructura_atributos is not null) THEN
        RAISE EXCEPTION_4;
    end if;

    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN ID ESTRUCTURA_VALOR EN CLAVE_VALOR
    ------------------------------------------------------------------------------------------------------------------------------------
    */

    select TOP 1 clave_valor into:v_estructura_values
    from silver.estructura_valor
    group by clave_valor
    having count(*) > 1;

    if (v_estructura_values is not null) THEN
        RAISE EXCEPTION_5;
    end if;

    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN ID IRE_ATRIBUTOS EN CLAVE_IRE
    ------------------------------------------------------------------------------------------------------------------------------------
    */

    select TOP 1 clave_ire into: v_ire_atributos
    from silver.ire_atributos
    group by clave_ire
    having count(*) > 1;

    if (v_ire_atributos is not null) THEN
        RAISE EXCEPTION_6;
    end if;

    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN ID IRE_VALOR EN CLAVE_VALOR
    ------------------------------------------------------------------------------------------------------------------------------------
    */

    select TOP 1 clave_valor into: v_ire_valor
    from silver.ire_valor
    group by clave_valor
    having count(*) > 1;

    if (v_ire_valor is not null) THEN
        RAISE EXCEPTION_7;
    end if;


    RETURN 'OK';  -- salida del SP
EXCEPTION

    WHEN EXCEPTION_1 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje) 
        values (
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de la suma de balance');
        RETURN SQLERRM;

    WHEN EXCEPTION_2 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de clave_valor en balance_values'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_3 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de clave_tech en balance_tech'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_4 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de clave_tech en estructura_atributos'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_5 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de clave_values en estructura_valor'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_6 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de clave_ire en ire_atributos'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_7 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_2','Error en la validación de clave_valor en ire_valor'
        );
        RETURN SQLERRM;

END;
$$;

--call SP_VALIDACION_2();

