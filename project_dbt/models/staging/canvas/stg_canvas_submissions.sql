

SELECT
    CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS int64) AS id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.assignment_id') AS int64) AS assignment_id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.grading_period_id') AS int64) AS grading_period_id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.user_id') AS int64) AS user_id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.grader_id') AS int64) AS grader_id,
    JSON_EXTRACT_SCALAR(data, '$.grade') AS grade,
    CAST(JSON_EXTRACT_SCALAR(data, '$.score') AS float64) AS score,
    JSON_EXTRACT_SCALAR(data, '$.entered_grade') AS entered_grade,
    CAST(JSON_EXTRACT_SCALAR(data, '$.entered_score') AS float64) AS entered_score,
    CAST(JSON_EXTRACT_SCALAR(data, '$.submitted_at') AS TIMESTAMP) AS submitted_at,
    CAST(JSON_EXTRACT_SCALAR(data, '$.graded_at') AS TIMESTAMP) AS graded_at,
    CAST(JSON_EXTRACT_SCALAR(data, '$.posted_at') AS TIMESTAMP) AS posted_at,
    JSON_EXTRACT_SCALAR(data, '$.submission_type') AS submission_type,
    CAST(JSON_EXTRACT_SCALAR(data, '$.grade_matches_current_submission') AS BOOL) AS grade_matches_current_submission,
    CAST(JSON_EXTRACT_SCALAR(data, '$.late') AS BOOL) AS late,
    CAST(JSON_EXTRACT_SCALAR(data, '$.missing') AS BOOL) AS missing,
    JSON_EXTRACT_SCALAR(data, '$.workflow_state') AS workflow_state
FROM {{ source('raw_sources', 'canvas_submissions') }} 