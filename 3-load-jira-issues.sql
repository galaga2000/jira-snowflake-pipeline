CREATE OR REPLACE PROCEDURE fetch_raw_jira_issues_to_snowflake()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
-- TODO: Replace with your actual external access integration name
EXTERNAL_ACCESS_INTEGRATIONS = (JIRA_API_Integration) 
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
-- TODO: Replace with your actual secret name containing JIRA credentials
SECRETS = ('cred' = jira_api_secret) 
LOG_LEVEL   = INFO
AS
$$
import _snowflake
import json
import math
import requests
import pandas as pd
import logging
import uuid
from snowflake.snowpark import Session

# ─── Setup logging ─────────────────────────────────────────────────────────────
logging.basicConfig()
base_logger = logging.getLogger('jira_loader')
base_logger.setLevel(logging.INFO)
run_id = str(uuid.uuid4())
logger = logging.LoggerAdapter(base_logger, {"run_id": run_id, "job": "jira_loader"})

session_client = requests.Session()

def fetch_jira_issues(username, token, jql_query, batch_size=1000):
    # TODO: Replace <tenant> with your actual Atlassian tenant name
    api_url = "https://<tenant>.atlassian.net/rest/api/3/search"
    headers = {"Accept": "application/json"}
    auth    = (username, token)
    fields  = "*all"

    # 1) Initial call to get total, effective page size, and first page
    params = {"jql": jql_query, "startAt": 0, "maxResults": batch_size, "fields": fields}
    init_rsp = session_client.get(api_url, headers=headers, auth=auth, params=params)
    init_rsp.raise_for_status()
    data = init_rsp.json()
    total          = data.get("total", 0)
    effective_page = data.get("maxResults", len(data.get("issues", [])))
    issues         = data.get("issues", [])

    logger.info(f"Total issues matching JQL: {total}", extra={"run_id": run_id})
    logger.info(f"JIRA honored maxResults={effective_page}", extra={"run_id": run_id})

    # 2) Page through the rest
    pages = math.ceil(total / effective_page)
    for i in range(1, pages):
        start = i * effective_page
        rsp = session_client.get(
            api_url,
            headers=headers,
            auth=auth,
            params={"jql": jql_query, "startAt": start,
                    "maxResults": effective_page, "fields": fields}
        )
        rsp.raise_for_status()
        batch = rsp.json().get("issues", [])
        issues.extend(batch)
        logger.info(f"Fetched page {i+1}/{pages}: {len(batch)} issues (startAt={start})",
                    extra={"run_id": run_id})
    return issues

def clean_empty_dicts(obj):
    """Recursively drop any dict that becomes empty."""
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            cleaned = clean_empty_dicts(v)
            # keep only non-empty dicts or other types
            if not (isinstance(cleaned, dict) and not cleaned):
                new[k] = cleaned
        return new
    elif isinstance(obj, list):
        return [clean_empty_dicts(v) for v in obj]
    else:
        return obj

def write_raw_json_to_snowflake(session: Session, issues: list, table_name: str):
    # 1) Truncate existing transient table (schema preserved)
    session.sql(f"TRUNCATE TABLE {table_name}").collect()
    logger.info(f"Truncated table {table_name}", extra={"run_id": run_id})

    # 2) Deep-clean each issue of empty dicts
    cleaned = [clean_empty_dicts(issue) for issue in issues]

    # 3) Build DataFrame and append
    df = pd.DataFrame([{"KEY": itm["key"], "RAW_DATA": itm} for itm in cleaned])
    session.write_pandas(
        df,
        table_name,
        auto_create_table=False,
        overwrite=False,
        table_type="TRANSIENT",
        quote_identifiers=True
    )
    logger.info(f"Wrote {len(df)} records to {table_name}", extra={"run_id": run_id})

def main(session: Session):
    # Load creds from secret
    secret   = _snowflake.get_generic_secret_string('cred')
    creds    = json.loads(secret)
    username = creds['username']
    token    = creds['token']

    # TODO: Replace PROJECT_NAME with your JIRA project key
    jql_query = 'project=PROJECT_NAME AND timespent IS NOT EMPTY AND created >= "2024-07-01"'

    # Fetch, clean, and load
    issues = fetch_jira_issues(username, token, jql_query)
    # TODO: Replace RAW_JIRA_ISSUES with your target Snowflake table name
    write_raw_json_to_snowflake(session, issues, "RAW_JIRA_ISSUES")

    logger.info(f"Completed load: {len(issues)} issues", extra={"run_id": run_id})
    return f"Loaded {len(issues)} issues; run_id={run_id}"
$$;
