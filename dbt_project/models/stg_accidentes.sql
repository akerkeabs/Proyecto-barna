WITH source AS (
    SELECT * FROM {{ source('raw_data', 'stg_accidentes_github') }}
),

limpieza AS (
    SELECT
        numero_expedient AS id_expediente,

        CAST(codi_districte AS INTEGER) AS id_distrito,
        nom_districte AS nombre_distrito,
        CAST(codi_barri AS INTEGER) AS id_barrio,
        nom_barri AS nombre_barrio,
        nom_carrer AS nombre_calle,
        num_postal AS codigo_postal,
        CAST(longitud AS FLOAT) AS longitud,
        CAST(latitud AS FLOAT) AS latitud,

        -- Fechas y Tiempo
        CAST(nk_any AS INTEGER) AS anio,
        CAST(mes_any AS INTEGER) AS mes,
        nom_mes AS nombre_mes,
        CAST(dia_mes AS INTEGER) AS dia,
        CAST(hora_dia AS INTEGER) AS hora,
        descripcio_dia_setmana AS dia_semana,
        descripcio_torn AS turno,

        -- Detalles del Accidente
        causa AS causa_accidente,

        -- Metadatos de Airflow
        etl_fecha_ingesta

    FROM source
)

SELECT * FROM limpieza