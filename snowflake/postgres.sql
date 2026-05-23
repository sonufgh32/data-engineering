USE ROLE ACCOUNTADMIN;

-- =========================================================
-- 0. Create Database & Push Postgres Image
-- =========================================================

CREATE DATABASE IF NOT EXISTS POSTGRES_DB;

-- docker pull postgres:16
-- docker login dtodzzj-tk32106.registry.snowflakecomputing.com -u SONUFGH32
-- docker tag postgres:16 dtodzzj-tk32106.registry.snowflakecomputing.com/postgres_db/public/postgres_repo/postgres:16
-- docker push dtodzzj-tk32106.registry.snowflakecomputing.com/postgres_db/public/postgres_repo/postgres:16


-- =========================================================
-- 1. Create Compute Pool
-- =========================================================

CREATE COMPUTE POOL IF NOT EXISTS POSTGRES_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_RESUME = TRUE
  AUTO_SUSPEND_SECS = 3600;

-- =========================================================
-- 2. Create Image Repository
-- =========================================================

CREATE IMAGE REPOSITORY IF NOT EXISTS POSTGRES_DB.PUBLIC.POSTGRES_REPO;

-- =========================================================
-- 3. Create Service
-- =========================================================

CREATE SERVICE POSTGRES_SERVICE
  IN COMPUTE POOL POSTGRES_POOL
  FROM SPECIFICATION $$
spec:
  containers:
    - name: postgres
      image: dtodzzj-tk32106.registry.snowflakecomputing.com/postgres_db/public/postgres_repo/postgres:16
      env:
        POSTGRES_USER: admin
        POSTGRES_PASSWORD: Password123
        POSTGRES_DB: appdb

  endpoints:
    - name: postgres-endpoint
      port: 5432
      protocol: TCP
$$;


GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;

GRANT CREATE IMAGE REPOSITORY ON SCHEMA POSTGRES_DB.PUBLIC TO ROLE DATA_ENGINEER_ROLE;

-- =========================================================
-- Cleanup: Drop all resources
-- =========================================================

SHOW ENDPOINTS IN SERVICE POSTGRES_DB.PUBLIC.POSTGRES_SERVICE;

DROP SERVICE IF EXISTS POSTGRES_DB.PUBLIC.POSTGRES_SERVICE;
DROP IMAGE REPOSITORY IF EXISTS POSTGRES_DB.PUBLIC.POSTGRES_REPO;
DROP COMPUTE POOL IF EXISTS POSTGRES_POOL;
DROP DATABASE IF EXISTS POSTGRES_DB;