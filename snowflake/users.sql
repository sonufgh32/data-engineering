DROP USER IF EXISTS data_analyst;
DROP USER IF EXISTS data_engineer;
DROP USER IF EXISTS database_admin;
DROP USER IF EXISTS prod_support;
DROP USER IF EXISTS etl_service;

CREATE USER data_analyst
PASSWORD = 'data_analyst@1234'
FIRST_NAME = 'Shiv Ram'
LAST_NAME = 'Choudhury'
EMAIL = 'choudhuryshivram@gmail.com'
DEFAULT_ROLE = data_analyst_role
MUST_CHANGE_PASSWORD = FALSE;

CREATE USER data_engineer
PASSWORD = 'data_engineer@1234'
FIRST_NAME = 'Shiv Ram'
LAST_NAME = 'Choudhury'
EMAIL = 'choudhuryshivram@gmail.com'
DEFAULT_ROLE = data_engineer_role
MUST_CHANGE_PASSWORD = FALSE;

CREATE USER database_admin
PASSWORD = 'database_admin@1234'
FIRST_NAME = 'Shiv Ram'
LAST_NAME = 'Choudhury'
EMAIL = 'choudhuryshivram@gmail.com'
DEFAULT_ROLE = dba_role
MUST_CHANGE_PASSWORD = FALSE;

CREATE USER prod_support
PASSWORD = 'prod_support@1234'
FIRST_NAME = 'Shiv Ram'
LAST_NAME = 'Choudhury'
EMAIL = 'choudhuryshivram@gmail.com'
DEFAULT_ROLE = prod_support_role
MUST_CHANGE_PASSWORD = FALSE;

CREATE USER etl_service
TYPE = SERVICE
EMAIL = 'sonufgh32@gmail.com'
DEFAULT_ROLE = ETL_ROLE
COMMENT = 'Service account for ETL pipelines';

GRANT ROLE data_analyst_role TO USER data_analyst;
GRANT ROLE data_engineer_role TO USER data_engineer;
GRANT ROLE dba_role TO USER database_admin;
GRANT ROLE prod_support_role TO USER prod_support;
GRANT ROLE ETL_ROLE TO USER etl_service;

ALTER USER data_analyst SET DEFAULT_WAREHOUSE = data_analytics_wh;
ALTER USER data_engineer SET DEFAULT_WAREHOUSE = data_engineer_wh;


GRANT ROLE data_engineer_role TO USER data_analyst;
REVOKE ROLE data_engineer_role FROM USER data_analyst;

GRANT USAGE ON DATABASE SNOWFLAKE_LEARNING_DB TO ROLE data_analyst_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE data_engineer_role;

SHOW GRANTS ON DATABASE SNOWFLAKE_LEARNING_DB;
SHOW GRANTS ON SCHEMA SNOWFLAKE_LEARNING_DB.INFORMATION_SCHEMA;
SHOW GRANTS ON TABLE SNOWFLAKE_LEARNING_DB.INFORMATION_SCHEMA.APPLICABLE_ROLES;


GRANT ROLE data_analyst_role, data_engineer_role, dba_role, prod_support_role TO USER sonufgh32;


SHOW GRANTS TO ROLE data_engineer_role;

SHOW USERS;