-- =========================================================
-- SNOWFLAKE TASK EXAMPLES
-- =========================================================
-- Database : SAMPLE_ENTERPRISE_DB
-- Schemas  : SALES, HR
-- Includes:
--   1. Scheduled Task
--   2. Task Chain
--   3. Serverless Task
--   4. Warehouse Based Task
--   5. Cron Based Task
-- =========================================================

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;

USE ROLE DATA_ENGINEER_ROLE;

USE WAREHOUSE DATA_ENGINEER_WH;

USE DATABASE SAMPLE_ENTERPRISE_DB;

-- =========================================================
-- SCHEMA : SALES
-- =========================================================

USE SCHEMA SALES;

-- =========================================================
-- TASK 1 : INSERT DAILY ORDERS
-- =========================================================
-- Runs every day at 8 AM IST

SELECT COUNT(*) AS CURRENT_ORDERS FROM SAMPLE_ENTERPRISE_DB.SALES.ORDERS;
-- 20 AS OF 9:30 AM 10-05-2026
CREATE OR REPLACE TASK DAILY_ORDER_LOAD
WAREHOUSE = data_engineer_wh
SCHEDULE = 'USING CRON 0 8 * * * Asia/Kolkata'
AS

INSERT INTO SAMPLE_ENTERPRISE_DB.SALES.ORDERS
SELECT
    SEQ4() + 2000,
    UNIFORM(1, 20, RANDOM()),
    CURRENT_DATE,
    UNIFORM(500, 10000, RANDOM()),
    'PENDING'
FROM TABLE(GENERATOR(ROWCOUNT => 5));

-- Show Task
SHOW TASKS LIKE 'DAILY_ORDER_LOAD';

-- Start Task
ALTER TASK SAMPLE_ENTERPRISE_DB.SALES.DAILY_ORDER_LOAD RESUME;

-- =========================================================
-- TASK 2 : UPDATE SHIPMENT STATUS
-- =========================================================
-- Runs every 2 hours

CREATE OR REPLACE TASK UPDATE_SHIPMENT_TASK
WAREHOUSE = data_engineer_wh
SCHEDULE = '120 MINUTE'
AS

UPDATE SHIPMENTS
SET DELIVERY_DATE = CURRENT_DATE + 2
WHERE DELIVERY_DATE < CURRENT_DATE;

ALTER TASK UPDATE_SHIPMENT_TASK RESUME;

-- =========================================================
-- TASK 3 : TASK CHAIN ROOT TASK
-- =========================================================

SELECT COUNT(*) AS CURRENT_PAYMENTS FROM SAMPLE_ENTERPRISE_DB.SALES.PAYMENTS;
-- 20 AS OF 9:32 AM 10-05-2026

CREATE OR REPLACE TASK ROOT_SALES_TASK
WAREHOUSE = data_engineer_wh
SCHEDULE = '60 MINUTE'
AS

INSERT INTO SAMPLE_ENTERPRISE_DB.SALES.PAYMENTS
SELECT
    SEQ4() + 9000,
    UNIFORM(1001, 1020, RANDOM()),
    'UPI',
    UNIFORM(100, 5000, RANDOM()),
    CURRENT_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 3));

ALTER TASK ROOT_SALES_TASK RESUME;

-- =========================================================
-- TASK 4 : CHILD TASK
-- =========================================================
-- Runs after ROOT_SALES_TASK completes

SELECT * FROM SAMPLE_ENTERPRISE_DB.SALES.PAYMENTS;

SELECT
    PAYMENT_MODE,
    COUNT(*) AS TOTAL_PAYMENTS,
    SUM(PAYMENT_AMOUNT) AS TOTAL_AMOUNT
FROM SAMPLE_ENTERPRISE_DB.SALES.PAYMENTS
GROUP BY PAYMENT_MODE;

CREATE OR REPLACE TASK CHILD_PAYMENT_REPORT_TASK
WAREHOUSE = data_engineer_wh
AFTER ROOT_SALES_TASK
AS

CREATE OR REPLACE TABLE PAYMENT_SUMMARY AS
SELECT
    PAYMENT_MODE,
    COUNT(*) AS TOTAL_PAYMENTS,
    SUM(PAYMENT_AMOUNT) AS TOTAL_AMOUNT
FROM SAMPLE_ENTERPRISE_DB.SALES.PAYMENTS
GROUP BY PAYMENT_MODE;

ALTER TASK CHILD_PAYMENT_REPORT_TASK RESUME;

-- =========================================================
-- SCHEMA : HR
-- =========================================================

USE SCHEMA HR;

-- =========================================================
-- TASK 5 : DAILY ATTENDANCE LOAD
-- =========================================================

SELECT COUNT(*) AS CURRENT_ATTENDANCE FROM ATTENDANCE;
-- 20 AS OF 9:35 AM 10-05-2026

CREATE OR REPLACE TASK DAILY_ATTENDANCE_TASK
WAREHOUSE = data_engineer_wh
SCHEDULE = 'USING CRON 30 22 * * * Asia/Kolkata'
AS

INSERT INTO ATTENDANCE
SELECT
    SEQ4() + 100,
    UNIFORM(1, 20, RANDOM()),
    CURRENT_DATE,
    'PRESENT'
FROM TABLE(GENERATOR(ROWCOUNT => 20));

ALTER TASK DAILY_ATTENDANCE_TASK RESUME;

-- =========================================================
-- TASK 6 : MONTHLY PAYROLL TASK
-- =========================================================

SELECT
    SEQ4() + 100,
    UNIFORM(1, 20, RANDOM()),
    TO_VARCHAR(CURRENT_DATE, 'YYYY-MM'),
    UNIFORM(30000, 120000, RANDOM()),
    UNIFORM(1000, 15000, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

CREATE OR REPLACE TASK MONTHLY_PAYROLL_TASK
WAREHOUSE = data_engineer_wh
SCHEDULE = 'USING CRON 0 1 1 * * Asia/Kolkata'
AS

INSERT INTO PAYROLL
SELECT
    SEQ4() + 100,
    UNIFORM(1, 20, RANDOM()),
    TO_VARCHAR(CURRENT_DATE, 'YYYY-MM'),
    UNIFORM(30000, 120000, RANDOM()),
    UNIFORM(1000, 15000, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

ALTER TASK MONTHLY_PAYROLL_TASK RESUME;

-- =========================================================
-- TASK 7 : SERVERLESS TASK
-- =========================================================
-- No warehouse required

CREATE OR REPLACE TASK SERVERLESS_EMPLOYEE_REPORT
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '1440 MINUTE'
AS

CREATE OR REPLACE TABLE EMPLOYEE_REPORT AS
SELECT
    DEPARTMENT,
    COUNT(*) AS TOTAL_EMPLOYEES,
    AVG(SALARY) AS AVG_SALARY
FROM EMPLOYEES
GROUP BY DEPARTMENT;

ALTER TASK SERVERLESS_EMPLOYEE_REPORT RESUME;

-- =========================================================
-- SHOW TASKS
-- =========================================================

SHOW TASKS;

-- =========================================================
-- CHECK TASK HISTORY
-- =========================================================

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE DATABASE_NAME = 'SAMPLE_ENTERPRISE_DB'
ORDER BY SCHEDULED_TIME DESC;

-- =========================================================
-- MANUALLY EXECUTE TASK
-- =========================================================

EXECUTE TASK SAMPLE_ENTERPRISE_DB.SALES.DAILY_ORDER_LOAD;

-- =========================================================
-- SUSPEND TASK
-- =========================================================

ALTER TASK SAMPLE_ENTERPRISE_DB.SALES.DAILY_ORDER_LOAD SUSPEND;

-- =========================================================
-- RESUME TASK
-- =========================================================

ALTER TASK SAMPLE_ENTERPRISE_DB.SALES.DAILY_ORDER_LOAD RESUME;


SELECT SCHEDULED_TIME, STATE, ERROR_CODE, ERROR_MESSAGE, QUERY_TEXT 
FROM TABLE(SAMPLE_ENTERPRISE_DB.INFORMATION_SCHEMA.TASK_HISTORY(
  TASK_NAME => 'DAILY_ORDER_LOAD',
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
)) 
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;