
SELECT
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS int64) AS id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.course_id') AS int64) AS course_id,
    JSON_EXTRACT_SCALAR(data, '$.name') AS name,
    JSON_EXTRACT_SCALAR(data, '$.description') AS description,
    CAST(JSON_EXTRACT_SCALAR(data, '$.due_at') AS TIMESTAMP) AS due_at,
    CAST(JSON_EXTRACT_SCALAR(data, '$.points_possible') AS float64) AS points_possible,
    JSON_EXTRACT_SCALAR(data, '$.workflow_state') AS workflow_state
FROM {{ source('raw_sources', 'canvas_assignments') }}
