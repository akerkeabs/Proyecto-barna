WITH accidentes AS (
    SELECT * FROM {{ ref('stg_accidentes') }}
),

agrupacion AS (
    SELECT
        anio,
        id_distrito,
        nombre_distrito,
        COUNT(id_expediente) AS total_accidentes
    FROM accidentes
    -- Filtramos los distritos con "-1" que son los "Desconegut" (Desconocidos)
    WHERE id_distrito != -1
    GROUP BY
        anio,
        id_distrito,
        nombre_distrito
)

SELECT *
FROM agrupacion
ORDER BY anio DESC, total_accidentes DESC