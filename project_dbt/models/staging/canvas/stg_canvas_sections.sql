
SELECT
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS int64) AS id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.course_id') AS int64) AS course_id,
    JSON_EXTRACT_SCALAR(data, '$.sis_section_id') AS sis_section_id,
    JSON_EXTRACT_SCALAR(data, '$.sis_course_id') AS sis_course_id,
    JSON_EXTRACT_SCALAR(data, '$.name') AS name,
    CAST(JSON_EXTRACT_SCALAR(data, '$.total_students') AS int64) AS total_students,
    CAST(JSON_EXTRACT_SCALAR(data, '$.restrict_enrollments_to_section_dates') AS BOOL) AS restrict_enrollments_to_section_dates,
    JSON_EXTRACT_SCALAR(data, '$.workflow_state') AS workflow_state,
    CAST(JSON_EXTRACT_SCALAR(data, '$.start_at') AS TIMESTAMP) AS start_at,
    CAST(JSON_EXTRACT_SCALAR(data, '$.end_at') AS TIMESTAMP) AS end_at,
    CAST(JSON_EXTRACT_SCALAR(data, '$.created_at') AS TIMESTAMP) AS created_at
FROM {{ source('raw_sources', 'canvas_sections') }}
