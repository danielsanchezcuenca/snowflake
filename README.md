# Proyecto: Pipeline de Datos con Snowflake y GCS

Este proyecto muestra cómo diseñar e implementar un pipeline de datos moderno en **Snowflake**, ingiriendo ficheros JSON desde **Google Cloud Storage** y aplicando el patrón **Bronze → Silver → Gold**.

---

## 📂 Estructura del repositorio

SNOWFLAKE/
├── bronze/
│   ├── carga_bronze.sql
│   ├── SP_RUN_BRONZE.sql
│   └── SP_VALIDACION_CARGA.sql
├── silver/
│   ├── carga_balance.sql
│   ├── carga_estructura.sql
│   ├── carga_ire.sql
│   ├── gold.sql
│   ├── SP_RUN_SILVER.sql
│   ├── SP_VALIDATE_SILVER.sql
│   └── SP_RUN_GOLD.sql
├── control/
│   └── control.sql
├── carga/
│   ├── config.sql
│   └── gcs.sql
└── README.md


---

## 🔹 1. Conexión y Stage

- **Storage integration** con Google Cloud Storage (`GCS_INT`).
- Creación de `ST_RAW` apuntando a `gcs://…` con `FILE FORMAT` en JSON.
- `LIST @ST_RAW;` para comprobar la disponibilidad de ficheros.

---

## 🔹 2. Capa Bronze

- Tablas de ingesta “peladas”, con columnas de trazabilidad:
  - `raw`, `src_file`, `src_dt`, `load_ts`, `raw_hash`.
- Función `PATH_TO_DATE` para extraer fechas del path.
- Cargas con `COPY INTO` a tablas temporales.
- `MERGE` idempotente para evitar duplicados exactos por fichero y hash.

---

## 🔹 3. Capa Silver

- Transformación de los JSON en columnas tipadas (`NUMBER`, `DATE`, `STRING`).
- Scripts de carga por entidad (`balance`, `estructura`, `ire`).
- Procedimientos `SP_RUN_SILVER` y validaciones integradas (`SP_VALIDATE_SILVER`).

---

## 🔹 4. Capa Gold

- Consolidación y métricas de negocio.
- `MERGE` de actualización final.
- Procedimiento `SP_RUN_GOLD`.

---

## 🔹 5. Orquestación y Control

- Procedimientos `SP_RUN_BRONZE`, `SP_RUN_SILVER`, `SP_RUN_GOLD` permiten ejecutar el pipeline completo.
- Manejo de excepciones con inserción en `CTRL.LOGS`.
- Ejecución secuencial con control de errores.

---

## ✅ Buenas prácticas aplicadas

- **Idempotencia:** cargas repetibles sin duplicados.
- **Trazabilidad:** cada fila conserva fuente, fecha y hash.
- **Control de errores:** logs centralizados para auditoría.
- **Orquestación SQL:** SPs encapsulan la lógica de negocio y técnica.

---

## 🚀 Próximos pasos

- Automatización con **tasks encadenados**.
- Visualización en BI o dashboards.
- Extensión del modelo a nuevas entidades.

---

#Snowflake #GCP #DataEngineering #BronzeSilverGold #CloudData #ETL

