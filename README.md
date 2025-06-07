# JIRA to Snowflake Integration

This repository contains SQL scripts and a Python stored procedure for loading JIRA issues into Snowflake and querying execution logs. Follow the steps below to set up, run, and monitor the integration.

## Files

1. **`1-jira-external-integration.sql`**

   * Creates a network rule (`JIRA_API_Rule`)
   * Creates an External Access Integration (`JIRA_API_Integration`)
   * Stores JIRA credentials in a secret (`jira_api_secret`)

2. **`2-create-landing-table.sql`**

   * Creates a transient landing table `RAW_JIRA_ISSUES_DEV`
   * Schema:

     ```sql
     CREATE OR REPLACE TRANSIENT TABLE RAW_JIRA_ISSUES_DEV (
       "KEY" STRING,
       "RAW_DATA" VARIANT
     );
     ```

3. **`3-load-jira-issues.sql`**

   * Defines the Python-based stored procedure `fetch_raw_jira_issues_to_snowflake_batch`
   * Fetches all issues via JIRA REST API (batched and cleaned)
   * Truncates and loads data into `RAW_JIRA_ISSUES_DEV`
   * Emits INFO-level logs to `SNOWFLAKE.TELEMETRY.EVENTS` with a unique `run_id`

4. **`4-call-load-jira-issues.sql`**

   * Executes the stored procedure to perform the data load:

     ```sql
     CALL fetch_raw_jira_issues_to_snowflake_batch();
     ```

5. **`query-logs.sql`**

   * Query to retrieve procedure execution logs:

     ```sql
     SELECT
       TIMESTAMP,
       RECORD['severity_text'] AS level,
       SCOPE['name']           AS logger_name,
       VALUE                   AS message,
       RECORD_ATTRIBUTES:run_id AS run_id,
       RECORD_ATTRIBUTES:job    AS job
     FROM SNOWFLAKE.TELEMETRY.EVENTS
     WHERE RECORD_TYPE = 'LOG'
       AND RECORD_ATTRIBUTES:job    = 'jira_loader'
       AND RECORD_ATTRIBUTES:run_id = '<YOUR_RUN_ID>'
     ORDER BY TIMESTAMP DESC;

     -- Verify loaded data
     SELECT * FROM RAW_JIRA_ISSUES_DEV;
     ```

## Prerequisites

* Snowflake role with privileges:

  * `CREATE NETWORK RULE`, `CREATE EXTERNAL ACCESS INTEGRATION`, `CREATE SECRET`
  * `CREATE TABLE`, `CREATE PROCEDURE`, `USAGE` on the target database/schema
* **Snowpark** enabled in your Snowflake account
* JIRA Cloud credentials (username + API token)

## Setup & Execution

1. **Configure External Access**

   ```sql
   -- creates network rule, integration, and secret
   @1-jira-external-integration.sql
   ```

2. **Create Landing Table**

   ```sql
   @2-create-landing-table.sql
   ```

3. **Deploy the Stored Procedure**

   ```sql
   @3-load-jira-issues.sql
   ```

   * Update placeholders in the script:

     * `<tenant>` in JIRA API URL
     * `jira_api_secret` contains your JIRA credentials
     * JQL filter for your project
     * Target table name if different

4. **Run the Data Load**

   ```sql
   @4-call-load-jira-issues.sql
   ```

   * The procedure returns:

     ```text
     Loaded <N> issues; run_id=<UUID>
     ```

5. **Query Logs & Data**

   ```sql
   -- replace <YOUR_RUN_ID> with the returned UUID
   @query-logs.sql
   ```

## Monitoring & Troubleshooting

* **Logs** are captured in `SNOWFLAKE.TELEMETRY.EVENTS` at INFO level and above.
* Use the returned `run_id` to filter log entries and trace execution steps.
* Verify row counts by querying `RAW_JIRA_ISSUES_DEV`.

---

*Generated on 2025-06-07*
