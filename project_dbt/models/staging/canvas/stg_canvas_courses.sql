
SELECT
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS int64) AS id,
    JSON_EXTRACT_SCALAR(data, '$.name') AS name,
    CAST(JSON_EXTRACT_SCALAR(data, '$.total_students') AS int64) AS total_students,
    JSON_EXTRACT_SCALAR(data, '$.workflow_state') AS workflow_state
FROM {{ source('raw_sources', 'canvas_courses') }}
