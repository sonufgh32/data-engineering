-- =========================================================
-- SNOWFLAKE SAMPLE DATABASE SETUP
-- =========================================================
-- Database      : SAMPLE_ENTERPRISE_DB
-- Schemas       : SALES, HR
-- Tables/schema : 5
-- Records/table : 20+
-- =========================================================

CREATE OR REPLACE DATABASE SAMPLE_ENTERPRISE_DB;

USE DATABASE SAMPLE_ENTERPRISE_DB;

-- =========================================================
-- SCHEMA : SALES
-- =========================================================

CREATE OR REPLACE SCHEMA SALES;

USE SCHEMA SALES;

-- =========================================================
-- TABLE 1 : CUSTOMERS
-- =========================================================

CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING,
    EMAIL STRING,
    CITY STRING,
    COUNTRY STRING
);

INSERT INTO CUSTOMERS
SELECT
    SEQ4() + 1,
    'Customer_' || (SEQ4() + 1),
    'customer' || (SEQ4() + 1) || '@example.com',
    CASE MOD(SEQ4(),5)
        WHEN 0 THEN 'Delhi'
        WHEN 1 THEN 'Mumbai'
        WHEN 2 THEN 'Hyderabad'
        WHEN 3 THEN 'Bangalore'
        ELSE 'Chennai'
    END,
    'India'
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 2 : PRODUCTS
-- =========================================================

CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID INT,
    PRODUCT_NAME STRING,
    CATEGORY STRING,
    PRICE NUMBER(10,2),
    STOCK_QTY INT
);

INSERT INTO PRODUCTS
SELECT
    SEQ4() + 1,
    'Product_' || (SEQ4() + 1),
    CASE MOD(SEQ4(),4)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Furniture'
        WHEN 2 THEN 'Accessories'
        ELSE 'Office'
    END,
    UNIFORM(100, 5000, RANDOM()),
    UNIFORM(10, 500, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 3 : ORDERS
-- =========================================================

CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID INT,
    CUSTOMER_ID INT,
    ORDER_DATE DATE,
    ORDER_AMOUNT NUMBER(10,2),
    STATUS STRING
);

INSERT INTO ORDERS
SELECT
    SEQ4() + 1001,
    UNIFORM(1, 20, RANDOM()),
    CURRENT_DATE - UNIFORM(1, 100, RANDOM()),
    UNIFORM(500, 10000, RANDOM()),
    CASE MOD(SEQ4(),4)
        WHEN 0 THEN 'PENDING'
        WHEN 1 THEN 'SHIPPED'
        WHEN 2 THEN 'DELIVERED'
        ELSE 'CANCELLED'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 4 : PAYMENTS
-- =========================================================

CREATE OR REPLACE TABLE PAYMENTS (
    PAYMENT_ID INT,
    ORDER_ID INT,
    PAYMENT_MODE STRING,
    PAYMENT_AMOUNT NUMBER(10,2),
    PAYMENT_DATE DATE
);

INSERT INTO PAYMENTS
SELECT
    SEQ4() + 5001,
    UNIFORM(1001, 1020, RANDOM()),
    CASE MOD(SEQ4(),4)
        WHEN 0 THEN 'UPI'
        WHEN 1 THEN 'CARD'
        WHEN 2 THEN 'NETBANKING'
        ELSE 'CASH'
    END,
    UNIFORM(500, 10000, RANDOM()),
    CURRENT_DATE - UNIFORM(1, 60, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 5 : SHIPMENTS
-- =========================================================

CREATE OR REPLACE TABLE SHIPMENTS (
    SHIPMENT_ID INT,
    ORDER_ID INT,
    SHIPMENT_DATE DATE,
    DELIVERY_DATE DATE,
    COURIER_NAME STRING
);

INSERT INTO SHIPMENTS
SELECT
    SEQ4() + 7001,
    UNIFORM(1001, 1020, RANDOM()),
    CURRENT_DATE - UNIFORM(1, 30, RANDOM()),
    CURRENT_DATE + UNIFORM(1, 10, RANDOM()),
    CASE MOD(SEQ4(),4)
        WHEN 0 THEN 'BlueDart'
        WHEN 1 THEN 'Delhivery'
        WHEN 2 THEN 'DTDC'
        ELSE 'EcomExpress'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- SCHEMA : HR
-- =========================================================

CREATE OR REPLACE SCHEMA HR;

USE SCHEMA HR;

-- =========================================================
-- TABLE 1 : EMPLOYEES
-- =========================================================

CREATE OR REPLACE TABLE EMPLOYEES (
    EMP_ID INT,
    EMP_NAME STRING,
    DEPARTMENT STRING,
    SALARY NUMBER(10,2),
    JOIN_DATE DATE
);

INSERT INTO EMPLOYEES
SELECT
    SEQ4() + 1,
    'Employee_' || (SEQ4() + 1),
    CASE MOD(SEQ4(),5)
        WHEN 0 THEN 'IT'
        WHEN 1 THEN 'HR'
        WHEN 2 THEN 'Finance'
        WHEN 3 THEN 'Operations'
        ELSE 'Support'
    END,
    UNIFORM(30000, 150000, RANDOM()),
    CURRENT_DATE - UNIFORM(100, 2000, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 2 : DEPARTMENTS
-- =========================================================

CREATE OR REPLACE TABLE DEPARTMENTS (
    DEPT_ID INT,
    DEPT_NAME STRING,
    LOCATION STRING,
    MANAGER_NAME STRING
);

INSERT INTO DEPARTMENTS
SELECT
    SEQ4() + 1,
    'Department_' || (SEQ4() + 1),
    CASE MOD(SEQ4(),5)
        WHEN 0 THEN 'Hyderabad'
        WHEN 1 THEN 'Bangalore'
        WHEN 2 THEN 'Mumbai'
        WHEN 3 THEN 'Pune'
        ELSE 'Chennai'
    END,
    'Manager_' || (SEQ4() + 1)
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 3 : ATTENDANCE
-- =========================================================

CREATE OR REPLACE TABLE ATTENDANCE (
    ATTENDANCE_ID INT,
    EMP_ID INT,
    ATTENDANCE_DATE DATE,
    STATUS STRING
);

INSERT INTO ATTENDANCE
SELECT
    SEQ4() + 1,
    UNIFORM(1, 20, RANDOM()),
    CURRENT_DATE - UNIFORM(1, 30, RANDOM()),
    CASE MOD(SEQ4(),3)
        WHEN 0 THEN 'PRESENT'
        WHEN 1 THEN 'ABSENT'
        ELSE 'WFH'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 4 : PAYROLL
-- =========================================================

CREATE OR REPLACE TABLE PAYROLL (
    PAYROLL_ID INT,
    EMP_ID INT,
    PAY_MONTH STRING,
    BASIC_SALARY NUMBER(10,2),
    BONUS NUMBER(10,2)
);

INSERT INTO PAYROLL
SELECT
    SEQ4() + 1,
    UNIFORM(1, 20, RANDOM()),
    '2026-04',
    UNIFORM(30000, 120000, RANDOM()),
    UNIFORM(1000, 20000, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- TABLE 5 : PROJECTS
-- =========================================================

CREATE OR REPLACE TABLE PROJECTS (
    PROJECT_ID INT,
    PROJECT_NAME STRING,
    EMP_ID INT,
    START_DATE DATE,
    END_DATE DATE
);

INSERT INTO PROJECTS
SELECT
    SEQ4() + 1,
    'Project_' || (SEQ4() + 1),
    UNIFORM(1, 20, RANDOM()),
    CURRENT_DATE - UNIFORM(10, 100, RANDOM()),
    CURRENT_DATE + UNIFORM(10, 200, RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- =========================================================
-- VALIDATION QUERIES
-- =========================================================

SELECT COUNT(*) FROM SALES.CUSTOMERS;

SELECT COUNT(*) FROM SALES.PRODUCTS;
SELECT COUNT(*) FROM SALES.ORDERS;
SELECT COUNT(*) FROM SALES.PAYMENTS;
SELECT COUNT(*) FROM SALES.SHIPMENTS;

SELECT COUNT(*) FROM HR.EMPLOYEES;
SELECT COUNT(*) FROM HR.DEPARTMENTS;
SELECT COUNT(*) FROM HR.ATTENDANCE;
SELECT COUNT(*) FROM HR.PAYROLL;
SELECT COUNT(*) FROM HR.PROJECTS;