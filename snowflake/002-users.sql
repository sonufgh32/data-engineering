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

SHOW USERS;