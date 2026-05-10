-- =========================================================
-- SNOWFLAKE STREAM EXAMPLES
-- =========================================================
-- Database : SAMPLE_ENTERPRISE_DB
-- Schema   : SALES
-- Includes:
--   1. Standard Stream
--   2. Insert Tracking
--   3. Update Tracking
--   4. Delete Tracking
--   5. Append Only Stream
--   6. Stream + Task Example
-- =========================================================

USE ROLE DATA_ENGINEER_ROLE;

USE DATABASE SAMPLE_ENTERPRISE_DB;

USE SCHEMA SALES;

-- =========================================================
-- CREATE SOURCE TABLE
-- =========================================================

CREATE OR REPLACE TABLE SALES_TRANSACTIONS (
    TRANSACTION_ID INT,
    CUSTOMER_NAME STRING,
    PRODUCT_NAME STRING,
    AMOUNT NUMBER(10,2),
    CREATED_AT TIMESTAMP
);

INSERT INTO SALES_TRANSACTIONS VALUES
(1, 'Rahul', 'Laptop', 55000, CURRENT_TIMESTAMP),
(2, 'Amit', 'Phone', 25000, CURRENT_TIMESTAMP),
(3, 'Neha', 'Monitor', 18000, CURRENT_TIMESTAMP);

SELECT * FROM SALES_TRANSACTIONS;

-- =========================================================
-- 1. CREATE STANDARD STREAM
-- =========================================================

CREATE OR REPLACE STREAM SALES_STREAM
ON TABLE SALES_TRANSACTIONS;

-- =========================================================
-- CHECK STREAM DATA
-- =========================================================

SELECT * FROM SALES_STREAM;

-- =========================================================
-- 2. INSERT TRACKING
-- =========================================================

INSERT INTO SALES_TRANSACTIONS VALUES
(4, 'Priya', 'Keyboard', 3000, CURRENT_TIMESTAMP),
(5, 'Karan', 'Mouse', 1500, CURRENT_TIMESTAMP);

-- Stream captures inserts
SELECT * FROM SALES_STREAM;

-- Metadata columns explanation:
-- METADATA$ACTION     -> INSERT / DELETE
-- METADATA$ISUPDATE   -> TRUE/FALSE
-- METADATA$ROW_ID     -> Internal row identifier

-- =========================================================
-- 3. UPDATE TRACKING
-- =========================================================

UPDATE SALES_TRANSACTIONS
SET AMOUNT = 60000
WHERE TRANSACTION_ID = 1;

SELECT * FROM SALES_STREAM;

-- For UPDATE:
-- Snowflake records:
--   1 DELETE record
--   1 INSERT record

-- =========================================================
-- 4. DELETE TRACKING
-- =========================================================

DELETE FROM SALES_TRANSACTIONS
WHERE TRANSACTION_ID = 2;

SELECT * FROM SALES_STREAM;

-- =========================================================
-- 5. CONSUME STREAM DATA
-- =========================================================
-- Once consumed in DML, offsets move forward

CREATE OR REPLACE TABLE SALES_AUDIT (
    TRANSACTION_ID INT,
    CUSTOMER_NAME STRING,
    PRODUCT_NAME STRING,
    AMOUNT NUMBER(10,2),
    CREATED_AT TIMESTAMP,
    ACTION_TYPE STRING
);

INSERT INTO SALES_AUDIT
SELECT
    TRANSACTION_ID,
    CUSTOMER_NAME,
    PRODUCT_NAME,
    AMOUNT,
    CREATED_AT,
    METADATA$ACTION
FROM SALES_STREAM;

-- After consumption
SELECT * FROM SALES_STREAM;

-- =========================================================
-- 6. APPEND ONLY STREAM
-- =========================================================
-- Tracks only INSERT operations

CREATE OR REPLACE STREAM SALES_APPEND_STREAM
ON TABLE SALES_TRANSACTIONS
APPEND_ONLY = TRUE;

INSERT INTO SALES_TRANSACTIONS VALUES
(6, 'Ankit', 'Tablet', 22000, CURRENT_TIMESTAMP);

SELECT * FROM SALES_APPEND_STREAM;

-- Updates and deletes are ignored

-- =========================================================
-- 7. STREAM ON VIEW
-- =========================================================

CREATE OR REPLACE VIEW HIGH_VALUE_SALES AS
SELECT *
FROM SALES_TRANSACTIONS
WHERE AMOUNT > 20000;

CREATE OR REPLACE STREAM HIGH_VALUE_STREAM
ON VIEW HIGH_VALUE_SALES;

SELECT * FROM HIGH_VALUE_STREAM;

-- =========================================================
-- 8. STREAM + TASK EXAMPLE
-- =========================================================

CREATE OR REPLACE TABLE SALES_HISTORY (
    TRANSACTION_ID INT,
    CUSTOMER_NAME STRING,
    PRODUCT_NAME STRING,
    AMOUNT NUMBER(10,2),
    CREATED_AT TIMESTAMP,
    ACTION_TYPE STRING
);

CREATE OR REPLACE TASK PROCESS_SALES_STREAM
WAREHOUSE = DATA_ENGINEER_WH
SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('SALES_STREAM')
AS

INSERT INTO SALES_HISTORY
SELECT
    TRANSACTION_ID,
    CUSTOMER_NAME,
    PRODUCT_NAME,
    AMOUNT,
    CREATED_AT,
    METADATA$ACTION
FROM SALES_STREAM;

ALTER TASK PROCESS_SALES_STREAM RESUME;


SELECT * FROM SALES_HISTORY;

-- =========================================================
-- 9. CHECK STREAM STATUS
-- =========================================================

SHOW STREAMS;

-- =========================================================
-- 10. DESCRIBE STREAM
-- =========================================================

DESCRIBE STREAM SALES_STREAM;

-- =========================================================
-- 11. CHECK IF STREAM HAS DATA
-- =========================================================

SELECT SYSTEM$STREAM_HAS_DATA('SALES_STREAM');

-- =========================================================
-- 12. DROP STREAM
-- =========================================================

CREATE OR REPLACE STREAM TEMP_STREAM
ON TABLE SALES_TRANSACTIONS;

DROP STREAM TEMP_STREAM;