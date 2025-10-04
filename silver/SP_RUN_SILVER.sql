CREATE OR REPLACE PROCEDURE SILVER.SP_RUN_SILVER()
RETURNS STRING  -- o otro tipo si aplica
LANGUAGE SQL    -- puede ser SQL o JAVASCRIPT
AS
$$
DECLARE
    EXCEPTION_1 EXCEPTION (-20001, 'ERROR EN LA CARGA DE DATOS EN BALANCE');
    EXCEPTION_2 EXCEPTION (-20001, 'ERROR EN LA CARGA DE DATOS EN ESTRUCTURA');
    EXCEPTION_3 EXCEPTION (-20001, 'ERROR EN LA CARGA DE DATOS EN IRE');
    EXCEPTION_4 EXCEPTION (-20001, 'ERROR EN LA VALIDACIÓN');
BEGIN
    
    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    BALANCE
    ------------------------------------------------------------------------------------------------------------------------------------
    */

    BEGIN
        EXECUTE IMMEDIATE FROM @MY_DB.PUBLIC.code_stage/silver/carga_balance.sql;
    EXCEPTION
        WHEN OTHER THEN 
            INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
            values(
                current_timestamp(),'carga_datos_balance.sql','ERROR EN LA CARGA DE DATOS EN BALANCE'
            );
            RAISE EXCEPTION_1;
        END;
     /*
    ------------------------------------------------------------------------------------------------------------------------------------
    ESTRUCTURA
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    BEGIN
        EXECUTE IMMEDIATE FROM @MY_DB.PUBLIC.code_stage/silver/carga_estructura.sql;
    EXCEPTION
        WHEN OTHER THEN 
            INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
            values(
                current_timestamp(),'carga_datos_estructura.sql','ERROR EN LA CARGA DE DATOS EN ESTRUCTURA'
            );
            RAISE EXCEPTION_2;
        END;

     /*
    ------------------------------------------------------------------------------------------------------------------------------------
    IRE
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    BEGIN
        EXECUTE IMMEDIATE FROM @MY_DB.PUBLIC.code_stage/silver/carga_ire.sql;
    EXCEPTION
        WHEN OTHER THEN 
            INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
            values(
                current_timestamp(),'carga_datos_ire.sql','ERROR EN LA CARGA DE DATOS EN IRE'
            );
            RAISE EXCEPTION_3;
        END;
     /*
    ------------------------------------------------------------------------------------------------------------------------------------
    VALIDACIÓN
    ------------------------------------------------------------------------------------------------------------------------------------
    */
    BEGIN
        CALL SP_VALIDACION_2();
    EXCEPTION
        WHEN OTHER THEN 
            INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
            values(
                current_timestamp(),'SP_VALIDACION_2','ERROR EN LA VALIDACIÓN DE SILVER'
            );
            RAISE EXCEPTION_4;
        END;

    RETURN 'ESTÁN ACTUALIZADOS LAS TABLAS EN SILVER';  -- salida del SP
EXCEPTION
    WHEN EXCEPTION_1 THEN
        RETURN 'ERROR EN LA CARGA DE DATOS EN BALANCE';
    WHEN EXCEPTION_2 THEN
        RETURN 'ERROR EN LA CARGA DE DATOS EN ESTRUCTURA';
    WHEN EXCEPTION_3 THEN
        RETURN 'ERROR EN LA CARGA DE DATOS EN IRE';
    WHEN EXCEPTION_4 THEN
        RETURN 'ERROR EN LA VALIDACIÓN DE DATOS';
END;
$$;


