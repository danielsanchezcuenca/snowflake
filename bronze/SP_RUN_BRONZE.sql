CREATE OR REPLACE PROCEDURE BRONZE.SP_RUN_BRONZE()
RETURNS STRING  -- o otro tipo si aplica
LANGUAGE SQL    -- puede ser SQL o JAVASCRIPT
AS
$$
DECLARE
    EXCEPTION_1 EXCEPTION (-20001, 'ERROR EN LA CARGA DE DATOS EN BRONZE');
    EXCEPTION_2 EXCEPTION (-20001, 'ERROR EN LA VALIDACIÓN DE DATOS EN BRONZE');
BEGIN
    
    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    REALIZAMOS LA CARGA Y CAPTURAMOS EL ERROR EN CASO DE QUE SUCEDA
    ------------------------------------------------------------------------------------------------------------------------------------
    */

    BEGIN
        EXECUTE IMMEDIATE FROM @MY_DB.PUBLIC.code_stage/bronze/carga_bronze_v2/carga_bronze.sql;
    EXCEPTION
        WHEN OTHER THEN 
            INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
            values(
                current_timestamp(),'carga_bronze.sql','ERROR EN LA CARGA DE DATOS EN BRONZE'
            );
            RAISE EXCEPTION_1;
        END;
    --SI FALLA SE INTERRUMPE LA EJECUCIÓN Y VA AL EXCEPTION OTHER

    BEGIN
        call BRONZE.SP_VALIDACION_CARGA();
    EXCEPTION
        WHEN OTHER THEN 
            INSERT INTO CTRL.LOGS(ts,procedimiento,mensaje)
            values(
                current_timestamp(),'SP_VALIDACION_CARGA','ERROR EN LA VALIDACIÓN DE DATOS EN BRONZE'
            );
            RAISE EXCEPTION_2;
        END;


    /*
    ------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------
    */


    RETURN 'ESTÁN ACTUALIZADOS LAS TABLAS EN BRONZE';  -- salida del SP
EXCEPTION
    WHEN EXCEPTION_1 THEN
        RETURN 'Error en la carga de datos en bronze';
    WHEN EXCEPTION_2 THEN
        RETURN 'Error en la validación de datos en bronze';
END;
$$;
