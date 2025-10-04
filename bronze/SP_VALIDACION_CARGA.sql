CREATE OR REPLACE PROCEDURE BRONZE.SP_VALIDACION_CARGA()
RETURNS STRING  -- o otro tipo si aplica
LANGUAGE SQL    -- puede ser SQL o JAVASCRIPT
AS
$$
DECLARE
    -- variables locales
    v_balance_dt BOOLEAN;
    v_balance_hash BOOLEAN;
    v_estructura_dt BOOLEAN;
    v_estructura_hash BOOLEAN;
    v_ire_dt BOOLEAN;
    v_ire_hash BOOLEAN;
    EXCEPTION_1 EXCEPTION (-20001, 'Error en la validación de la balance dt');
    EXCEPTION_2 EXCEPTION (-20001, 'Error en la validación de balance hash');
    EXCEPTION_3 EXCEPTION (-20001, 'Error en la validación estructura dt');
    EXCEPTION_4 EXCEPTION (-20001, 'Error en la validación de estructura hash');
    EXCEPTION_5 EXCEPTION (-20001, 'Error en la validación de ire dt');
    EXCEPTION_6 EXCEPTION (-20001, 'Error en la validación de ire hash');
BEGIN
    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN DE TODAS LAS FUENTES
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    With checks as(
    select 
        not exists (select 1  from bronze_balance group by src_dt having count(*) > 1) as balance_dt,
        not exists (select 1  from bronze_balance group by raw_hash having count(*) > 1) as balance_hash ,
        not exists (select 1  from bronze_estructura group by src_dt having count(*) > 1) as estructura_dt ,
        not exists (select 1  from bronze_ire group by raw_hash having count(*) > 1) as estructura_hash ,
        not exists (select 1 from bronze_ire group by src_dt having count(*) > 1) as ire_dt ,
        not exists (select 1  from bronze_ire group by raw_hash having count(*) > 1) as ire_hash
        )
    select balance_dt,
           balance_hash,
           estructura_dt,
           estructura_hash,
           ire_dt,
           ire_hash
    into v_balance_dt,
         v_balance_hash,
         v_estructura_dt,
         v_estructura_hash,
         v_ire_dt,
         v_ire_hash
    from checks;


    if (v_balance_dt <> TRUE) THEN
        RAISE EXCEPTION_1;
    end if;
    if (v_balance_hash <> true) THEN
        RAISE EXCEPTION_2;
    end if;
    if (v_estructura_dt <> true) THEN
        RAISE EXCEPTION_3;
    end if;
    if (v_estructura_hash <> true) THEN
        RAISE EXCEPTION_4;
    end if;
    if (v_ire_dt <> true) THEN
        RAISE EXCEPTION_5;
    end if; 
    if (v_ire_hash <> true) THEN
        RAISE EXCEPTION_6;
    end if;
    
    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------
    */


    RETURN 'OK';  -- salida del SP
EXCEPTION

    WHEN EXCEPTION_1 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje) 
        values (
                current_timestamp(),'SP_VALIDACION_CARGA','Error en la validación de la balance dt');
        RETURN SQLERRM;

    WHEN EXCEPTION_2 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_CARGA','Error en la validación de balance hash'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_3 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_CARGA','Error en la validación estructura dt'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_4 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_CARGA','Error en la validación de estructura hash'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_5 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_CARGA','Error en la validación de ire dt'
        );
        RETURN SQLERRM;

    WHEN EXCEPTION_6 THEN
        INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
        values(
                current_timestamp(),'SP_VALIDACION_CARGA','Error en la validación de ire hash'
        );
        RETURN SQLERRM;

END;
$$;





