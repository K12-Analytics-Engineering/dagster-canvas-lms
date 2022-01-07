
SELECT
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS int64) AS id,
    JSON_EXTRACT_SCALAR(data, '$.name') AS name,
    CAST(JSON_EXTRACT_SCALAR(data, '$.start_at') AS TIMESTAMP) AS start_at,
    CAST(JSON_EXTRACT_SCALAR(data, '$.end') AS TIMESTAMP) AS end_at,
    JSON_EXTRACT_SCALAR(data, '$.workflow_state') AS workflow_state
FROM {{ source('raw_sources', 'canvas_terms') }} 
