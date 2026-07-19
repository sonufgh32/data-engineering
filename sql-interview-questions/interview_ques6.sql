-- CREATE TABLE EMPLOYEE (
--     EMPID     NUMBER,
--     ENAME     VARCHAR2(50) NOT NULL,
--     DEPTNO    NUMBER(2),
--     SALARY    NUMBER(10,2),
--     JOB       VARCHAR2(30)
-- );

-- INSERT ALL
-- INTO EMPLOYEE VALUES (103, 'Charlie', 30, 40000, 'CLERK')
-- INTO EMPLOYEE VALUES (109, 'Ian', 30, 61000, 'ANALYST')
-- INTO EMPLOYEE VALUES (106, 'Frank', 30, 46000, 'CLERK')
-- INTO EMPLOYEE VALUES (106, 'Frank', 30, 46000, 'CLERK')
-- INTO EMPLOYEE VALUES (106, 'Frank', 30, 46000, 'CLERK')
-- INTO EMPLOYEE VALUES (102, 'Bob', 20, 62000, 'ANALYST')
-- INTO EMPLOYEE VALUES (108, 'Helen', 20, 56000, 'SALESMAN')
-- INTO EMPLOYEE VALUES (105, 'Eve', 10, 52000, 'SALESMAN')
-- INTO EMPLOYEE VALUES (101, 'Alice', 10, 85000, 'MANAGER')
-- INTO EMPLOYEE VALUES (107, 'Grace', 10, 73000, 'MANAGER')
-- INTO EMPLOYEE VALUES (110, 'Jane', 30, 80000, 'MANAGER')
-- INTO EMPLOYEE VALUES (110, 'Jane', 30, 80000, 'MANAGER')
-- INTO EMPLOYEE VALUES (110, 'Jane', 30, 80000, 'MANAGER')
-- INTO EMPLOYEE VALUES (104, 'David', 10, 95000, 'PRESIDENT')
-- SELECT * FROM DUAL;

-- CREATE TABLE LOCATIONS (
--     LOCATION_ID      NUMBER(4) PRIMARY KEY,
--     STREET_ADDRESS   VARCHAR2(100),
--     POSTAL_CODE      VARCHAR2(20),
--     CITY             VARCHAR2(50) NOT NULL,
--     STATE_PROVINCE   VARCHAR2(50),
--     COUNTRY_ID       CHAR(2) NOT NULL
-- );

-- INSERT ALL
-- INTO LOCATIONS VALUES (1000, '1297 Via Cola di Rie', '989', 'Roma', NULL, 'IT')
-- INTO LOCATIONS VALUES (1100, '93091 Calle della Testa', '10934', 'Venice', NULL, 'IT')
-- INTO LOCATIONS VALUES (1200, '2017 Shinjuku-ku', '1689', 'Tokyo', 'Tokyo Prefecture', 'JP')
-- INTO LOCATIONS VALUES (1300, '9450 Kamiya-cho', '6823', 'Hiroshima', NULL, 'JP')
-- INTO LOCATIONS VALUES (1400, '2014 Jabberwocky Rd', '26192', 'Southlake', 'Texas', 'US')
-- INTO LOCATIONS VALUES (1500, '2011 Interiors Blvd', '99236', 'South San Francisco', 'California', 'US')
-- INTO LOCATIONS VALUES (1600, '2007 Zagora St', '50090', 'South Brunswick', 'New Jersey', 'US')
-- INTO LOCATIONS VALUES (1700, '2004 Charade Rd', '98199', 'Seattle', 'Washington', 'US')
-- INTO LOCATIONS VALUES (1800, '147 Spadina Ave', 'M5V 2L7', 'Toronto', 'Ontario', 'CA')
-- INTO LOCATIONS VALUES (1900, '6092 Boxwood St', 'YSW 9T2', 'Whitehorse', 'Yukon', 'CA')
-- INTO LOCATIONS VALUES (2000, '40-5-12 Laogianggen', '190518', 'Beijing', NULL, 'CN')
-- INTO LOCATIONS VALUES (2100, '1298 Vileparle (E)', '490231', 'Bombay', 'Maharashtra', 'IN')
-- INTO LOCATIONS VALUES (2200, '12-98 Victoria Street', '2901', 'Sydney', 'New South Wales', 'AU')
-- INTO LOCATIONS VALUES (2300, '198 Clementi North', '540198', 'Singapore', NULL, 'SG')
-- INTO LOCATIONS VALUES (2400, '8204 Arthur St', NULL, 'London', NULL, 'UK')
-- INTO LOCATIONS VALUES (2500, 'Magdalen Centre, The Oxford Science Park', 'OX9 9ZB', 'Oxford', 'Oxford', 'UK')
-- INTO LOCATIONS VALUES (2600, '9702 Chester Road', '9629850293', 'Stretford', 'Manchester', 'UK')
-- INTO LOCATIONS VALUES (2700, 'Schwanthalerstr. 7031', '80925', 'Munich', 'Bavaria', 'DE')
-- INTO LOCATIONS VALUES (2800, 'Rua Frei Caneca 1360', '01307-002', 'Sao Paulo', 'Sao Paulo', 'BR')
-- INTO LOCATIONS VALUES (2900, '20 Rue des Corps-Saints', '1730', 'Geneva', 'Geneve', 'CH')
-- INTO LOCATIONS VALUES (3000, 'Murtenstrasse 921', '3095', 'Bern', 'BE', 'CH')
-- INTO LOCATIONS VALUES (3100, 'Pieter Breughelstraat 837', '3029SK', 'Utrecht', 'Utrecht', 'NL')
-- INTO LOCATIONS VALUES (3200, 'Mariano Escobedo 9991', '11932', 'Mexico City', 'Distrito Federal', 'MX')
-- SELECT * FROM DUAL;

-- COMMIT;

SELECT * FROM EMPLOYEE;
SELECT * FROM LOCATIONS;

-- Remove duplicate values from employee table
-- CTID in Redshift, SYSTEM$UUID in Snowflake
WITH RES AS (
    SELECT
        E.*,
        ROW_NUMBER() OVER(PARTITION BY E.EMPID ORDER BY EMPID) AS RN
    FROM
        EMPLOYEE E
) SELECT * FROM RES WHERE RN = 1;

SELECT DISTINCT * FROM EMPLOYEE;

-- Write a query to find out duplicate values from employee
SELECT
    EMPID
FROM
    EMPLOYEE
GROUP BY EMPID
HAVING COUNT(*) > 1;

WITH RES AS(
    SELECT
        E.*,
        ROW_NUMBER() OVER(PARTITION BY EMPID ORDER BY EMPID) AS RN
    FROM
        EMPLOYEE E
)
SELECT * FROM RES WHERE RN = 2;

-- Write a query to find out highest earning employee based on each position
WITH RES AS(
    SELECT MAX(SALARY) AS MAXSAL, JOB FROM EMPLOYEE GROUP BY JOB
),
FINAL AS(
    SELECT
        E.EMPID,
        E.ENAME,
        E.JOB,
        E.SALARY,
        ROW_NUMBER() OVER(PARTITION BY EMPID ORDER BY EMPID) AS RN
    FROM
        EMPLOYEE E INNER JOIN RES R ON R.MAXSAL = E.SALARY
        AND R.JOB = E.JOB
)
SELECT EMPID, ENAME, JOB, SALARY
FROM FINAL WHERE RN = 1;

SELECT
    DISTINCT JOB,
    FIRST_VALUE(EMPID) OVER(PARTITION BY JOB ORDER BY SALARY DESC) AS EMPID,
    FIRST_VALUE(ENAME) OVER(PARTITION BY JOB ORDER BY SALARY DESC) AS ENAME,
    FIRST_VALUE(SALARY) OVER(PARTITION BY JOB ORDER BY SALARY DESC) AS SALARY
FROM
    EMPLOYEE ORDER BY EMPID;

-- Write a query to get the top 3 highest earning employee.
SELECT
    DISTINCT SALARY
FROM
    EMPLOYEE
ORDER BY SALARY DESC
FETCH FIRST 3 ROWS ONLY;

-- Write a query to get the top 3 lowest earning employee.
SELECT
    DISTINCT SALARY
FROM
    EMPLOYEE
ORDER BY SALARY
FETCH FIRST 3 ROWS ONLY;

-- Write a query to find out 2nd highest salary employee
SELECT * FROM EMPLOYEE ORDER BY SALARY DESC;

WITH RES AS(
    SELECT
        SALARY,
        DENSE_RANK() OVER(ORDER BY SALARY DESC) AS DRNK
    FROM
        EMPLOYEE
    ORDER BY SALARY DESC
)
SELECT
    *
FROM EMPLOYEE E INNER JOIN RES R ON E.SALARY = R.SALARY
AND R.DRNK = 2;

-- Write a query to get 2nd lowest earning employee
SELECT * FROM EMPLOYEE ORDER BY SALARY;

WITH RES AS(
    SELECT
        SALARY,
        DENSE_RANK() OVER(ORDER BY SALARY) AS DRNK
    FROM
        EMPLOYEE
    ORDER BY SALARY
)
SELECT
    DISTINCT
    *
FROM EMPLOYEE E INNER JOIN RES R ON E.SALARY = R.SALARY
AND R.DRNK = 2;

-- Write a query to get 2nd highest salary based on each department
WITH RES AS(
    SELECT
        DEPTNO,
        SALARY,
        DENSE_RANK() OVER(PARTITION BY DEPTNO
                            ORDER BY SALARY DESC) AS DRNK
    FROM
        EMPLOYEE
    ORDER BY DEPTNO, SALARY DESC
)
SELECT
    E.EMPID, E.ENAME, E.DEPTNO, E.SALARY, E.JOB
FROM EMPLOYEE E INNER JOIN RES R ON E.SALARY = R.SALARY
AND E.DEPTNO = R.DEPTNO AND R.DRNK = 2;

-- Write a query to get top 2 salary based on each department
WITH RES AS(
    SELECT
        DEPTNO,
        SALARY,
        DENSE_RANK() OVER(PARTITION BY DEPTNO ORDER BY SALARY DESC) AS DRNK
    FROM
        EMPLOYEE
    ORDER BY DEPTNO, SALARY DESC
)
SELECT DISTINCT * FROM RES WHERE DRNK <= 2;

-- Write a query to create a new table with same schema as employee table (only schema)
CREATE TABLE EMP_TMP AS SELECT * FROM EMPLOYEE WHERE 1 = 2;

SELECT * FROM EMP_TMP;

-- Create new table same like employee table(data + schema)
CREATE TABLE EMP_TEMP AS SELECT * FROM EMPLOYEE;

SELECT * FROM EMP_TEMP;

-- Write a query where employee name starts with letter 'A'
SELECT * FROM EMPLOYEE WHERE ENAME LIKE 'A%';

-- Write a query where name starts letter and ends letter is same
SELECT
    *
FROM
    EMPLOYEE E
WHERE
    LOWER(SUBSTR(ENAME, 1, 1)) = LOWER(SUBSTR(ENAME, LENGTH(ENAME), 1));

-- Write a query to get records in xml format
-- Method 1
SELECT
    XMLTYPE(
        CURSOR(
            SELECT * FROM EMPLOYEE
        )
    ).getClobVal() AS EMPLOYEE_XML
FROM dual;

-- Method 2
SELECT
    XMLELEMENT("Employee",
        XMLFOREST(
            EMPID AS "EmpID",
            ENAME AS "Name",
            DEPTNO AS "DeptNo",
            SALARY AS "Salary",
            JOB AS "Job"
        )
    ) AS EMPLOYEE_XML
FROM EMPLOYEE;

-- Method 3
SELECT
    XMLSERIALIZE(
        DOCUMENT XMLELEMENT("Employees",
            XMLAGG(
                XMLELEMENT("Employee",
                    XMLFOREST(
                        EMPID AS "EmpID",
                        ENAME AS "Name",
                        DEPTNO AS "DeptNo",
                        SALARY AS "Salary",
                        JOB AS "Job"
                    )
                )
            )
        ) AS CLOB
    ) AS ALL_EMPLOYEES_XML
FROM EMPLOYEE;


-- How to get current date
SELECT CURRENT_DATE, CURRENT_TIMESTAMP FROM DUAL;

-- Query to get current month
SELECT
    EXTRACT(MONTH FROM SYSTIMESTAMP) AS M1,
    TO_CHAR(SYSTIMESTAMP, 'MON') AS M2
FROM DUAL;

-- Query to get current year
SELECT
    EXTRACT(YEAR FROM SYSTIMESTAMP) AS Y1,
    TO_CHAR(SYSTIMESTAMP, 'YYYY') AS Y2
FROM DUAL;

--
-- CREATE TABLE CHOCOLATE (
--     CHOCOLATE_NAME   VARCHAR2(50),
--     MANUFACTURER     VARCHAR2(50)
-- );

-- INSERT ALL
-- INTO CHOCOLATE VALUES ('Dairy Milk', 'Cadbury')
-- INTO CHOCOLATE VALUES ('Kit Kat', 'Nestle')
-- INTO CHOCOLATE VALUES ('Perk', NULL)
-- INTO CHOCOLATE VALUES ('Munch', 'Nestle')
-- INTO CHOCOLATE VALUES ('5 Star', NULL)
-- INTO CHOCOLATE VALUES ('Snickers', 'Mars')
-- INTO CHOCOLATE VALUES ('Milky Way', NULL)
-- INTO CHOCOLATE VALUES ('Toblerone', 'MondelÄ“z')
-- INTO CHOCOLATE VALUES ('Bounty', NULL)
-- INTO CHOCOLATE VALUES ('Twix', 'Mars')
-- SELECT * FROM DUAL;

-- COMMIT;

SELECT * FROM chocolate;

WITH RES(N) AS(
    SELECT 1 FROM DUAL

    UNION ALL

    SELECT N + 1 FROM RES WHERE N < 10 
) SELECT * FROM RES;

WITH RES AS(
    SELECT
        CHOCOLATE_NAME,
        MANUFACTURER,
        ROW_NUMBER() OVER(ORDER BY CHOCOLATE_NAME) AS RN
    FROM
        CHOCOLATE
)
SELECT
    R.RN,
    R.CHOCOLATE_NAME,
    R.MANUFACTURER,
    -- PREV.MANUFACTURER AS PREV,
    -- NEXT.MANUFACTURER AS NEXT,
    PREV.RN AS PREV_RN,
    NEXT.RN AS NEXT_RN
FROM
    RES R
    LEFT JOIN RES PREV ON (PREV.RN = R.RN - 1)
    LEFT JOIN RES NEXT ON (NEXT.RN = R.RN + 1);
--