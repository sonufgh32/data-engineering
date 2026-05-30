
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