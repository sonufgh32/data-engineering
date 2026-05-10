-- =========================================================
-- SNOWFLAKE SEQUENCE EXAMPLES
-- =========================================================
-- Database : SAMPLE_ENTERPRISE_DB
-- =========================================================

USE DATABASE SAMPLE_ENTERPRISE_DB;

USE SCHEMA SALES;

-- =========================================================
-- 1. CREATE BASIC SEQUENCE
-- =========================================================

CREATE OR REPLACE SEQUENCE CUSTOMER_SEQ
START = 1
INCREMENT = 1;

-- Generate Values
SELECT CUSTOMER_SEQ.NEXTVAL;

SELECT
    CUSTOMER_SEQ.NEXTVAL AS GENERATED_ID
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- =========================================================
-- 2. USE SEQUENCE IN TABLE INSERT
-- =========================================================

CREATE OR REPLACE TABLE CUSTOMER_SEQ_TABLE (
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING
);

INSERT INTO CUSTOMER_SEQ_TABLE
SELECT
    CUSTOMER_SEQ.NEXTVAL,
    'Customer_' || SEQ4()
FROM TABLE(GENERATOR(ROWCOUNT => 20));

SELECT * FROM CUSTOMER_SEQ_TABLE;

-- =========================================================
-- 3. SEQUENCE WITH CUSTOM START VALUE
-- =========================================================

CREATE OR REPLACE SEQUENCE ORDER_SEQ
START = 1000
INCREMENT = 1;

SELECT ORDER_SEQ.NEXTVAL;

-- =========================================================
-- 4. SEQUENCE WITH NEGATIVE INCREMENT
-- =========================================================

CREATE OR REPLACE SEQUENCE REVERSE_SEQ
START = 100
INCREMENT = -5;

SELECT
    REVERSE_SEQ.NEXTVAL AS REVERSE_NUMBER
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- =========================================================
-- 5. SEQUENCE WITH LARGE INCREMENT
-- =========================================================

CREATE OR REPLACE SEQUENCE EVEN_SEQ
START = 2
INCREMENT = 2;

SELECT
    EVEN_SEQ.NEXTVAL AS EVEN_NUMBER
FROM TABLE(GENERATOR(ROWCOUNT => 10));

-- =========================================================
-- 6. AUTO GENERATE IDS USING DEFAULT
-- =========================================================

CREATE OR REPLACE SEQUENCE EMP_SEQ
START = 1
INCREMENT = 1;

CREATE OR REPLACE TABLE EMPLOYEE_AUTO_ID (
    EMP_ID INT DEFAULT EMP_SEQ.NEXTVAL,
    EMP_NAME STRING,
    DEPARTMENT STRING
);

INSERT INTO EMPLOYEE_AUTO_ID (EMP_NAME, DEPARTMENT)
VALUES
('Rahul', 'IT'),
('Amit', 'HR'),
('Neha', 'Finance');

SELECT * FROM EMPLOYEE_AUTO_ID;

-- =========================================================
-- 7. ALTER SEQUENCE
-- =========================================================

ALTER SEQUENCE EVEN_SEQ
SET INCREMENT = 10;

SELECT
    EVEN_SEQ.NEXTVAL AS UPDATED_SEQUENCE
FROM TABLE(GENERATOR(ROWCOUNT => 5));

-- =========================================================
-- 8. SHOW SEQUENCES
-- =========================================================

SHOW SEQUENCES;

-- =========================================================
-- 9. DESCRIBE SEQUENCE
-- =========================================================

DESCRIBE SEQUENCE CUSTOMER_SEQ;

-- =========================================================
-- 10. DROP SEQUENCE
-- =========================================================

CREATE OR REPLACE SEQUENCE TEMP_SEQ;

DROP SEQUENCE TEMP_SEQ;