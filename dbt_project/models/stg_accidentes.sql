WITH raw_accidentes AS (
    SELECT * FROM {{ source('minio_datalake', 'accidentes_delta') }}
)

SELECT
    numero_expedient AS Numero_expediente,

    CAST(codi_districte AS INTEGER) AS Codi_districte,
    nom_districte AS Nom_distrito,
    CAST(codi_barri AS INTEGER) AS Codi_barri,
    nom_barri AS Nom_barri,
    nom_carrer AS Nom_carrer,
    num_postal AS Num_postal,
    CAST(longitud AS FLOAT) AS Longitud,
    CAST(latitud AS FLOAT) AS Latitud,

    -- Fechas y Tiempo
    CAST(nk_any AS INTEGER) AS NK_Any,
    CAST(mes_any AS INTEGER) AS Mes_any,
    nom_mes AS Nom_mes,
    CAST(dia_mes AS INTEGER) AS Dia_mes,
    CAST(hora_dia AS INTEGER) AS Hora_dia,
    descripcio_dia_setmana AS Descripcio_dia_setmana,
    descripcio_torn AS Descripcio_torn,
    data AS Data,

    -- Detalles del Accidente
    causa AS Causa,

    -- Metadatos de Airflow
    etl_fecha_ingesta

FROM raw_accidentes