SELECT
  TIMESTAMP,
  RECORD['severity_text']    AS level,
  SCOPE['name']              AS logger_name,
  VALUE                      AS message,
  RECORD_ATTRIBUTES:run_id   AS run_id,
  RECORD_ATTRIBUTES:job      AS job
FROM SNOWFLAKE.TELEMETRY.EVENTS
WHERE RECORD_TYPE = 'LOG'
AND RECORD_ATTRIBUTES:job = 'jira_loader'
AND RECORD_ATTRIBUTES:run_id = '<query-run-id>'
ORDER BY TIMESTAMP desc;


select * from RAW_JIRA_ISSUES;