
SELECT * FROM SALES;

SELECT
    S.*,
    ROW_NUMBER() OVER(ORDER BY DEPARTMENT_ID) RN,
    RANK() OVER(ORDER BY DEPARTMENT_ID) AS RANK,
    DENSE_RANK() OVER(ORDER BY DEPARTMENT_ID) AS DENSE_RANK
FROM
    SALES S;

-- Method 1
WITH TEMP AS (
    SELECT
        department_id,
        MIN(TOTAL_SALES) MIN_SALES,
        MAX(TOTAL_SALES) MAX_SALES
    FROM
        SALES
        GROUP BY DEPARTMENT_ID
),
result as (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY t.department_id 
                    ORDER BY t.department_id) as rn,
        t.department_id,
        s1.employee_name as min_sales_employee,
        t.MIN_SALES,
        s2.employee_name as max_sales_employee,
        t.MAX_SALES
    from
        temp t inner join sales s1
        on (t.department_id = s1.department_id and t.MIN_SALES = s1.TOTAL_SALES)
        inner join sales s2
        on (t.department_id = s2.department_id and t.MAX_SALES = s2.TOTAL_SALES)
)
SELECT department_id, min_sales_employee, MIN_SALES, max_sales_employee,
MAX_SALES FROM result where rn = 1;

-- Method 2
WITH TEMP AS (
    SELECT
        department_id,
        MIN(TOTAL_SALES) MIN_SALES,
        MAX(TOTAL_SALES) MAX_SALES
    FROM
        SALES
        GROUP BY DEPARTMENT_ID
),
min_res AS (
    SELECT
        sales.department_id,
        employee_name AS min_sales_employee,
        total_sales AS min_sales
    FROM
        sales inner join temp t on (
            sales.department_id = t.department_id
            and sales.total_sales = t.min_sales)
),
max_res AS (
    SELECT
        sales.department_id,
        employee_name AS max_sales_employee,
        total_sales AS max_sales
    FROM
        sales inner join temp t on (
            sales.department_id = t.department_id
            and sales.total_sales = t.max_sales)
),
result as (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY min_res.department_id
            ORDER BY min_res.department_id) as rn,
        min_res.department_id,
        min_res.min_sales_employee,
        min_res.min_sales,
        max_res.max_sales_employee,
        max_res.max_sales
    FROM min_res
        JOIN max_res ON min_res.department_id = max_res.department_id
)
SELECT
    department_id,
    min_sales_employee,
    min_sales,
    max_sales_employee,
    max_sales
from
    result
where rn = 1;

-- Method 3
SELECT 
    DISTINCT department_id,
    FIRST_VALUE(employee_name) OVER (PARTITION BY department_id ORDER BY total_sales ASC) AS min_sales_employee, 
    FIRST_VALUE(total_sales) OVER (PARTITION BY department_id ORDER BY total_sales ASC) AS min_sales, 
    FIRST_VALUE(employee_name) OVER (PARTITION BY department_id ORDER BY total_sales DESC) AS max_sales_employee, 
    FIRST_VALUE(total_sales) OVER (PARTITION BY department_id ORDER BY total_sales DESC) AS max_sales
FROM sales;


SELECT 0/1 FROM DUAL;
SELECT 1/0 FROM DUAL;
SELECT 0/NULL FROM DUAL;
SELECT NULL/1 FROM DUAL;
SELECT NULL/NULL FROM DUAL;
SELECT 0/0 FROM DUAL;
SELECT 1000 * NULL FROM DUAl;

SELECT SYSDATE, SYSTIMESTAMP FROM DUAL;

WITH TEMP(DT) AS (
    SELECT SYSDATE AS DT FROM DUAL
    UNION ALL
    SELECT DT + INTERVAL '1' DAY FROM TEMP
        WHERE DT <= TO_DATE(SYSDATE + INTERVAL '10' DAY, 'DD-MM-YY')
)
SELECT * FROM TEMP;

WITH CNT(N) AS (
    SELECT 1 AS N FROM DUAL
    UNION ALL
    SELECT N + 1 FROM CNT WHERE N < 10
) SELECT * FROM CNT;

-- Query to reverse the order using ROW_NUMBER
WITH CNT(N) AS (
    SELECT 1 AS N FROM DUAL
    UNION ALL
    SELECT N + 1 FROM CNT WHERE N < 10
)
SELECT
    N,
    ROW_NUMBER() OVER(ORDER BY N DESC) AS RN
FROM CNT ORDER BY N;

-- JPMC (customer which place at least 2 orders and avg amt > 500)
SELECT * FROM CUSTOMER;
SELECT * FROM ORDERS;

WITH CUST AS (
    SELECT
        CUSTOMER_ID,
        COUNT(1) AS TOTAL_ORDERS,
        ROUND(AVG(ORDER_AMOUNT), 2) AS AVG_ORDER_AMOUNT
    FROM ORDERS
        GROUP BY CUSTOMER_ID
)
SELECT
    C.CUSTOMER_ID,
    C2.FIRST_NAME || ' ' || C2.LAST_NAME AS CUSTOMER_NAME,
    C.TOTAL_ORDERS,
    C.AVG_ORDER_AMOUNT
FROM
    CUST C JOIN CUSTOMER C2 ON C.CUSTOMER_ID = C2.CUSTOMER_ID
WHERE
    TOTAL_ORDERS > 2
    AND AVG_ORDER_AMOUNT > 500;