create or replace network rule JIRA_API_Rule
mode = 'EGRESS'
type = HOST_PORT
value_list = ('<tenant>.atlassian.net:443')
comment = 'Jira API Rule';

create or replace external access integration JIRA_API_Integration
allowed_network_rules = ('JIRA_API_Rule')
allowed_authentication_secrets = ('jira_api_secret')
enabled = true
comment = 'Integration to allow Snowflake procedures/functions to call JIRA API'

create or replace secret jira_api_secret
  type = generic_string
  secret_string = '{
    "username": <username>,
    "token": <token-from-atlassian>
  }'
comment = 'JIRA API credentials';



