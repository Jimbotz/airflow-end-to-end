{{ config(materialized='view') }}

-- 1. Contamos cuántos hay en la tabla cruda (raw)
WITH conteo_raw AS (
    SELECT COUNT(*) as total_raw 
    FROM {{ source('raw_data', 'raw_users') }}
),

-- 2. Sumamos los usuarios que agrupamos en nuestro modelo demográfico
conteo_analytics AS (
    SELECT SUM(total_usuarios) as total_procesado 
    FROM {{ ref('dim_demografia_usuarios') }}
)

-- 3. Juntamos ambas piezas y calculamos la diferencia
SELECT 
    CURRENT_TIMESTAMP as fecha_revision,
    r.total_raw as registros_en_postgres,
    COALESCE(a.total_procesado, 0) as registros_en_analytics,
    (r.total_raw - COALESCE(a.total_procesado, 0)) as diferencia_perdida
FROM conteo_raw r
CROSS JOIN conteo_analytics a