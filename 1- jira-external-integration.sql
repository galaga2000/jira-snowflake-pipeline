-- Example Snowflake configuration for JIRA integration
-- Replace the following placeholders with your actual values:
-- - Update the host in value_list with your JIRA tenant
-- - Create your own secret with actual credentials
-- - Update the integration name to match your environment

-- Network Rule Example
create or replace network rule JIRA_API_Rule
mode = 'EGRESS'
type = HOST_PORT
value_list = ('YOUR-TENANT.atlassian.net:443')
comment = 'Example Jira API Rule';

-- External Access Integration Example
create or replace external access integration JIRA_API_Integration
allowed_network_rules = ('JIRA_API_Rule')
allowed_authentication_secrets = ('YOUR_SECRET_NAME')
enabled = true
comment = 'Example integration for JIRA API access';

-- Secret Configuration Example
-- DO NOT use this secret configuration directly
-- Create your own secret with your actual credentials
-- create or replace secret YOUR_SECRET_NAME
--   type = generic_string
--   secret_string = '{
--     "username": "YOUR_USERNAME",
--     "token": "YOUR_API_TOKEN"
--   }'
-- comment = 'Your JIRA API credentials';



