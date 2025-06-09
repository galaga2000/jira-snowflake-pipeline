CREATE OR REPLACE PROCEDURE fetch_raw_jira_issues_to_snowflake()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (JIRA_API_Integration)
PACKAGES = ('snowflake-snowpark-python', 'requests')
SECRETS = ('cred' = jira_api_secret)
LOG_LEVEL = INFO
AS
$$
import _snowflake
import json
import math
import requests
import logging
import uuid
from snowflake.snowpark import Session

# Setup base logging
logging.basicConfig()
base_logger = logging.getLogger('jira_loader')
base_logger.setLevel(logging.INFO)

# Create unique run identifier for log tracing
run_id = str(uuid.uuid4())
logger = logging.LoggerAdapter(base_logger, {"run_id": run_id, "job": "jira_loader"})

# Create a persistent HTTP session
session_client = requests.Session()

def fetch_jira_issues(username, token, jql_query, batch_size=100):
    """
    Fetch issues from Jira API based on JQL query in batches.
    
    Args:
        username (str): Jira username or email
        token (str): Jira API token
        jql_query (str): JQL query string
        batch_size (int, optional): Number of issues to fetch per request (default 100)
    
    Returns:
        list: List of issues retrieved from Jira
    """

    # TODO: Replace <TENANT-NAME> with your actual Atlassian Cloud tenant domain
    api_url = "https://<TENANT-NAME>.atlassian.net/rest/api/3/search"
    headers = {"Accept": "application/json"}
    auth = (username, token)

    # TODO: Consider replacing '*all' with a comma-separated list of required fields for performance
    fields = "*all"

    # Initial API call to get the first page of results and total count
    params = {
        "jql": jql_query,
        "startAt": 0,
        "maxResults": batch_size,
        "fields": fields
    }

    init_rsp = session_client.get(api_url, headers=headers, auth=auth, params=params)
    init_rsp.raise_for_status()
    data = init_rsp.json()

    total = data.get("total", 0)
    effective_page = data.get("maxResults", len(data.get("issues", [])))
    issues = data.get("issues", [])

    logger.info(f"Total issues matching JQL: {total}", extra={"run_id": run_id})
    logger.info(f"Jira honored maxResults={effective_page}", extra={"run_id": run_id})

    # Paginate if needed
    pages = math.ceil(total / effective_page)
    for i in range(1, pages):
        start = i * effective_page
        rsp = session_client.get(
            api_url,
            headers=headers,
            auth=auth,
            params={
                "jql": jql_query,
                "startAt": start,
                "maxResults": effective_page,
                "fields": fields
            }
        )
        rsp.raise_for_status()
        batch = rsp.json().get("issues", [])
        issues.extend(batch)
        logger.info(f"Fetched page {i+1}/{pages}: {len(batch)} issues (startAt={start})", extra={"run_id": run_id})

    return issues

def clean_empty_dicts(obj):
    """
    Recursively remove empty dictionaries from a JSON-like object.

    Args:
        obj (dict or list): JSON object or list to clean

    Returns:
        Cleaned object with no empty dictionaries
    """
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            cleaned = clean_empty_dicts(v)
            if not (isinstance(cleaned, dict) and not cleaned):
                new[k] = cleaned
        return new
    elif isinstance(obj, list):
        return [clean_empty_dicts(v) for v in obj]
    else:
        return obj

def write_raw_json_to_snowflake(session: Session, issues: list, table_name: str, batch_size=10000):
    """
    Write list of Jira issues to a Snowflake table in batches.

    Args:
        session (Session): Snowpark Session object
        issues (list): List of issues to write
        table_name (str): Target Snowflake table name
        batch_size (int, optional): Number of rows per batch insert (default 10,000)
    """
    # Truncate the table to remove old data before loading new data
    session.sql(f"TRUNCATE TABLE {table_name}").collect()
    logger.info(f"Truncated table {table_name}", extra={"run_id": run_id})

    # Clean issues to remove empty dicts OPTIONAL
    cleaned = [clean_empty_dicts(issue) for issue in issues]

    # Insert data in batches to avoid memory issues
    n_batches = math.ceil(len(cleaned) / batch_size)
    logger.info(f"Total {len(cleaned)} issues to insert in {n_batches} batches", extra={"run_id": run_id})

    for i in range(n_batches):
        batch = cleaned[i * batch_size: (i + 1) * batch_size]
        # Keep RAW_DATA as dict (Snowpark will map it to VARIANT type automatically)
        data_batch = [{"KEY": itm["key"], "RAW_DATA": itm} for itm in batch]
        
        # Create Snowpark DataFrame and write to Snowflake
        snowpark_df = session.create_dataframe(data_batch)
        snowpark_df.write.mode("append").save_as_table(table_name)

        logger.info(f"Wrote batch {i+1}/{n_batches} with {len(batch)} records", extra={"run_id": run_id})

def main(session: Session):
    """
    Main procedure handler.
    
    Args:
        session (Session): Snowpark session provided by Snowflake
    
    Returns:
        str: Summary message of the load
    """
    # Retrieve credentials from Snowflake secrets
    secret = _snowflake.get_generic_secret_string('cred')
    creds = json.loads(secret)
    username = creds['username']
    token = creds['token']

    # TODO: Replace <PROJECT-NAME> with your actual project or use a dynamic input
    jql_query = 'project = "<PROJECT-NAME>"'  # Customize the JQL query as needed

    # Fetch Jira issues based on JQL
    issues = fetch_jira_issues(username, token, jql_query)

    # Write results to Snowflake table
    write_raw_json_to_snowflake(session, issues, "RAW_JIRA_ISSUES")

    logger.info(f"Completed load: {len(issues)} issues", extra={"run_id": run_id})
    return f"Loaded {len(issues)} issues; run_id={run_id}"
$$;
