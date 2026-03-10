{{ config(materialized='view') }}

-- Extraemos los últimos 100 registros basándonos en el orden temporal del ObjectID de Mongo
SELECT 
    mongo_id,
    nombre,
    apellido_paterno,
    apellido_materno,
    email,
    estatus
FROM {{ source('raw_data', 'raw_users') }}
ORDER BY mongo_id DESC
LIMIT 100