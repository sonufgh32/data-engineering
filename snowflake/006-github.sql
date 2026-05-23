USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE DEV_DB;
USE DATABASE DEV_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE API INTEGRATION github_integration
API_PROVIDER = git_https_api
API_ALLOWED_PREFIXES = ('https://github.com/sonufgh32/')
ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY my_repo
API_INTEGRATION = github_integration
ORIGIN = 'https://github.com/sonufgh32/data-engineering.git';

SHOW GIT REPOSITORIES;
DESCRIBE GIT REPOSITORY my_repo;

ALTER GIT REPOSITORY my_repo FETCH;

SHOW GIT BRANCHES IN my_repo;

LS @my_repo/branches/main;

-- EXECUTE IMMEDIATE FROM @my_repo/branches/main/snowflake/roles.sql;