-- =========================================================
-- STORED PROCEDURE EXAMPLES IN SNOWFLAKE
-- =========================================================
-- Database : SAMPLE_ENTERPRISE_DB
-- Schemas  : SALES, HR
-- Includes:
--   1. SQL Stored Procedure
--   2. Python Stored Procedure
--   3. Procedure with Parameters
--   4. Procedure Returning Table Data

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
-- 1. SQL STORED PROCEDURE
-- #########################################################
-- Insert a new customer

CREATE OR REPLACE PROCEDURE ADD_CUSTOMER(
    P_CUSTOMER_ID INT,
    P_CUSTOMER_NAME STRING,
    P_EMAIL STRING,
    P_CITY STRING,
    P_COUNTRY STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO CUSTOMERS
    VALUES (
        :P_CUSTOMER_ID,
        :P_CUSTOMER_NAME,
        :P_EMAIL,
        :P_CITY,
        :P_COUNTRY
    );

    RETURN 'Customer Inserted Successfully';

END;
$$;

-- Example Usage
CALL ADD_CUSTOMER(101, 'Rahul Verma', 'rahul@example.com', 'Hyderabad', 'India');

-- #########################################################
-- 2. SQL STORED PROCEDURE
-- #########################################################
-- Update product stock

CREATE OR REPLACE PROCEDURE UPDATE_PRODUCT_STOCK(
    P_PRODUCT_ID INT,
    P_NEW_STOCK INT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    UPDATE PRODUCTS
    SET STOCK_QTY = :P_NEW_STOCK
    WHERE PRODUCT_ID = :P_PRODUCT_ID;

    RETURN 'Stock Updated Successfully';

END;
$$;

-- Example Usage
CALL UPDATE_PRODUCT_STOCK(1, 250);

-- #########################################################
-- 3. PYTHON STORED PROCEDURE
-- #########################################################
-- Count employees by department

USE SCHEMA HR;

CREATE OR REPLACE PROCEDURE GET_EMPLOYEE_COUNT(
    P_DEPARTMENT STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, p_department):

    query = f"""
        SELECT COUNT(*) AS TOTAL_EMPLOYEES
        FROM EMPLOYEES
        WHERE DEPARTMENT = '{p_department}'
    """

    result = session.sql(query).collect()

    total = result[0]['TOTAL_EMPLOYEES']

    return f"Total Employees in {p_department}: {total}"
$$;

-- Example Usage
CALL GET_EMPLOYEE_COUNT('IT');

-- #########################################################
-- 4. PYTHON STORED PROCEDURE
-- #########################################################
-- Increase salary by percentage

CREATE OR REPLACE PROCEDURE INCREMENT_SALARY(
    P_DEPARTMENT STRING,
    P_PERCENT FLOAT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, p_department, p_percent):

    update_query = f"""
        UPDATE EMPLOYEES
        SET SALARY = SALARY + (SALARY * {p_percent} / 100)
        WHERE DEPARTMENT = '{p_department}'
    """

    session.sql(update_query).collect()

    return f"Salary updated for department: {p_department}"
$$;

-- Example Usage
CALL INCREMENT_SALARY('IT', 10);

-- #########################################################
-- 5. SQL STORED PROCEDURE
-- #########################################################
-- Return total order amount

USE SCHEMA SALES;

CREATE OR REPLACE PROCEDURE GET_TOTAL_ORDER_AMOUNT()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    TOTAL_AMOUNT NUMBER(12,2);

BEGIN

    SELECT SUM(ORDER_AMOUNT)
    INTO :TOTAL_AMOUNT
    FROM ORDERS;

    RETURN 'Total Order Amount: ' || TOTAL_AMOUNT;

END;
$$;

-- Example Usage
CALL GET_TOTAL_ORDER_AMOUNT();

-- #########################################################
-- 6. PYTHON STORED PROCEDURE
-- #########################################################
-- Get high salary employees

USE SCHEMA HR;

CREATE OR REPLACE PROCEDURE GET_HIGH_SALARY_EMPLOYEES(
    P_MIN_SALARY FLOAT
)
RETURNS TABLE (
    EMP_ID NUMBER(38,0),
    EMP_NAME VARCHAR,
    DEPARTMENT VARCHAR,
    SALARY NUMBER(10,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, p_min_salary):

    query = f"""
        SELECT
            EMP_ID,
            EMP_NAME,
            DEPARTMENT,
            SALARY
        FROM EMPLOYEES
        WHERE SALARY >= {p_min_salary}
    """

    return session.sql(query)
$$;

-- Example Usage
CALL GET_HIGH_SALARY_EMPLOYEES(90000);

-- #########################################################
-- SHOW ALL PROCEDURES
-- #########################################################

SHOW PROCEDURES;