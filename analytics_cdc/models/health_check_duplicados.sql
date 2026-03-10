{{ config(materialized='view') }}

-- Agrupamos por CURP y Email para detectar clones
SELECT 
    curp,
    email,
    COUNT(*) as cantidad_de_registros,
    ARRAY_AGG(mongo_id) as lista_de_ids_afectados
FROM {{ source('raw_data', 'raw_users') }}
GROUP BY curp, email
HAVING COUNT(*) > 1