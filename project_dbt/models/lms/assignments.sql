
SELECT
    canvas_assignments.assignments.id AS AssignmentIdentifier,
    "uri://instructure.com" AS Namespace,
    STRUCT(
        JSON_VALUE(data, '$.sectionReference.localCourseCode') AS local_course_code,
        JSON_VALUE(data, '$.sectionReference.schoolId') AS school_id,
        JSON_VALUE(data, '$.sectionReference.schoolYear') AS school_year,
        JSON_VALUE(data, '$.sectionReference.sectionIdentifier') AS section_identifier,
        JSON_VALUE(data, '$.sectionReference.sessionName') AS session_name
    ) AS section_reference,
FROM {{ ref('stg_canvas_assignments') }} canvas_assignments
LEFT JOIN {{ ref('edfi_sections') }} edfi_sections
    ON edfi_sections.section_identifier = canvas_assignments
