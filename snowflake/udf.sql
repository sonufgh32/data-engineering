-- =========================================================
-- UDF EXAMPLES IN SNOWFLAKE
-- =========================================================
-- Database : SAMPLE_ENTERPRISE_DB
-- Schemas  : SALES, HR
-- Includes:
--   1. SQL Scalar UDF
--   2. SQL Table UDF
--   3. Python Scalar UDF
--   4. Python Table UDF

-- In Snowflake, a UDF is used to return calculated values
-- inside SQL queries, while a Stored Procedure is used to
-- execute complex workflows and database operations like
-- INSERT, UPDATE, DELETE, or DDL statements.
-- =========================================================

USE DATABASE SAMPLE_ENTERPRISE_DB;

-- =========================================================
-- SCHEMA : SALES
-- =========================================================

USE SCHEMA SALES;

-- #########################################################
-- 1. SQL SCALAR UDF
-- #########################################################
-- Returns discounted amount

CREATE OR REPLACE FUNCTION CALCULATE_DISCOUNT(
    PRICE NUMBER(10,2),
    DISCOUNT_PERCENT NUMBER(5,2)
)
RETURNS NUMBER(10,2)
AS
$$
    PRICE - (PRICE * DISCOUNT_PERCENT / 100)
$$;

-- Example Usage
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    PRICE,
    CALCULATE_DISCOUNT(PRICE, 10) AS DISCOUNTED_PRICE
FROM PRODUCTS;

-- #########################################################
-- 2. SQL TABLE UDF
-- #########################################################
-- Returns orders above a given amount

CREATE OR REPLACE FUNCTION GET_HIGH_VALUE_ORDERS(
    MIN_AMOUNT NUMBER
)
RETURNS TABLE (
    ORDER_ID INT,
    CUSTOMER_ID INT,
    ORDER_AMOUNT NUMBER(10,2),
    STATUS STRING
)
AS
$$
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_AMOUNT,
        STATUS
    FROM ORDERS
    WHERE ORDER_AMOUNT >= MIN_AMOUNT
$$;

-- Example Usage
SELECT * FROM TABLE(GET_HIGH_VALUE_ORDERS(5000));

-- #########################################################
-- 3. PYTHON SCALAR UDF
-- #########################################################
-- Categorize salary into bands

USE SCHEMA HR;

CREATE OR REPLACE FUNCTION GET_SALARY_BAND(SALARY FLOAT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'salary_band'
AS
$$
def salary_band(salary):
    if salary < 50000:
        return "LOW"
    elif salary < 100000:
        return "MEDIUM"
    else:
        return "HIGH"
$$;

-- Example Usage
SELECT
    EMP_ID,
    EMP_NAME,
    SALARY,
    GET_SALARY_BAND(SALARY) AS SALARY_BAND
FROM EMPLOYEES;

-- #########################################################
-- 4. PYTHON TABLE UDF
-- #########################################################
-- Returns employee bonus calculations

CREATE OR REPLACE FUNCTION EMPLOYEE_BONUS_TABLE(
    BONUS_PERCENT FLOAT
)
RETURNS TABLE (
    EMP_ID INT,
    EMP_NAME STRING,
    SALARY FLOAT,
    BONUS_AMOUNT FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'EmployeeBonusHandler'
AS
$$
class EmployeeBonusHandler:

    def process(self, bonus_percent):

        employees = [
            (1, "Employee_1", 50000),
            (2, "Employee_2", 70000),
            (3, "Employee_3", 90000),
            (4, "Employee_4", 120000)
        ]

        for emp in employees:
            emp_id = emp[0]
            emp_name = emp[1]
            salary = emp[2]

            bonus_amount = salary * (bonus_percent / 100)

            yield (
                emp_id,
                emp_name,
                salary,
                bonus_amount
            )
$$;

-- Example Usage
SELECT * FROM TABLE(EMPLOYEE_BONUS_TABLE(15.0::FLOAT));

-- #########################################################
-- 5. SQL SCALAR UDF
-- #########################################################
-- Calculate yearly salary

CREATE OR REPLACE FUNCTION YEARLY_SALARY(
    MONTHLY_SALARY NUMBER(10,2)
)
RETURNS NUMBER(10,2)
AS
$$
    MONTHLY_SALARY * 12
$$;

-- Example Usage
SELECT
    EMP_ID,
    EMP_NAME,
    SALARY,
    YEARLY_SALARY(SALARY) AS ANNUAL_SALARY
FROM EMPLOYEES;

-- #########################################################
-- 6. SQL TABLE UDF
-- #########################################################
-- Return employees by department

CREATE OR REPLACE FUNCTION GET_EMPLOYEES_BY_DEPT(
    DEPT_NAME STRING
)
RETURNS TABLE (
    EMP_ID INT,
    EMP_NAME STRING,
    DEPARTMENT STRING,
    SALARY NUMBER(10,2)
)
AS
$$
    SELECT
        EMP_ID,
        EMP_NAME,
        DEPARTMENT,
        SALARY
    FROM EMPLOYEES
    WHERE DEPARTMENT = DEPT_NAME
$$;

-- Example Usage
SELECT * FROM TABLE(GET_EMPLOYEES_BY_DEPT('IT'));

-- #########################################################
-- SHOW ALL FUNCTIONS
-- #########################################################

SHOW USER FUNCTIONS;