{{ config(materialized='table') }}

WITH clasificacion_edades AS (
    SELECT 
        mongo_id,
        edad,
        estatus,
        CASE 
            WHEN edad BETWEEN 18 AND 27 THEN 'Gen Z'
            WHEN edad BETWEEN 28 AND 43 THEN 'Millennials'
            WHEN edad BETWEEN 44 AND 59 THEN 'Gen X'
            ELSE 'Boomers+'
        END AS generacion
    FROM {{ source('raw_data', 'raw_users') }}
)

SELECT 
    generacion,
    estatus,
    COUNT(*) as total_usuarios,
    ROUND(AVG(edad), 1) as edad_promedio
FROM clasificacion_edades
GROUP BY generacion, estatus
ORDER BY generacion, estatus