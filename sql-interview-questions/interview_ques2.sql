-- CREATE TABLE sales (
--     sales_id NUMBER PRIMARY KEY,
--     employee_name VARCHAR2(50),
--     department_id NUMBER,
--     total_sales NUMBER
-- );

-- INSERT INTO sales VALUES (1, 'Alice', 1, 5000);
-- INSERT INTO sales VALUES (2, 'Bob', 1, 7000);
-- INSERT INTO sales VALUES (3, 'Charlie', 2, 7000);
-- INSERT INTO sales VALUES (4, 'David', 2, 6000);
-- INSERT INTO sales VALUES (5, 'Eve', 1, 5000);
-- COMMIT;

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
-- CREATE TABLE CUSTOMER (
--     CUSTOMER_ID      NUMBER PRIMARY KEY,
--     FIRST_NAME       VARCHAR2(50) NOT NULL,
--     LAST_NAME        VARCHAR2(50) NOT NULL,
--     EMAIL            VARCHAR2(100) UNIQUE NOT NULL,
--     PHONE_NUMBER     VARCHAR2(15),
--     GENDER           VARCHAR2(10),
--     DATE_OF_BIRTH    DATE,
--     CITY             VARCHAR2(50),
--     STATE            VARCHAR2(50),
--     COUNTRY          VARCHAR2(50),
--     CREATED_AT       DATE
-- );

-- INSERT ALL
-- INTO CUSTOMER VALUES (1, 'Amit', 'Sharma', 'amit.sharma@email.com', '9876543210', 'Male', TO_DATE('15-05-1990','DD-MM-YYYY'), 'Mumbai', 'Maharashtra', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (2, 'Priya', 'Verma', 'priya.verma@email.com', '9876543211', 'Female', TO_DATE('21-07-1992','DD-MM-YYYY'), 'Delhi', 'Delhi', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (3, 'Rahul', 'Mehta', 'rahul.mehta@email.com', '9876543212', 'Male', TO_DATE('10-11-1988','DD-MM-YYYY'), 'Bangalore', 'Karnataka', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (4, 'Sneha', 'Patel', 'sneha.patel@email.com', '9876543213', 'Female', TO_DATE('09-03-1995','DD-MM-YYYY'), 'Ahmedabad', 'Gujarat', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (5, 'Arjun', 'Reddy', 'arjun.reddy@email.com', '9876543214', 'Male', TO_DATE('17-08-1991','DD-MM-YYYY'), 'Hyderabad', 'Telangana', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (6, 'Kavya', 'Nair', 'kavya.nair@email.com', '9876543215', 'Female', TO_DATE('25-12-1993','DD-MM-YYYY'), 'Kochi', 'Kerala', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (7, 'Vikram', 'Singh', 'vikram.singh@email.com', '9876543216', 'Male', TO_DATE('30-04-1987','DD-MM-YYYY'), 'Jaipur', 'Rajasthan', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (8, 'Neha', 'Kapoor', 'neha.kapoor@email.com', '9876543217', 'Female', TO_DATE('14-06-1996','DD-MM-YYYY'), 'Chandigarh', 'Punjab', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (9, 'Rohan', 'Das', 'rohan.das@email.com', '9876543218', 'Male', TO_DATE('05-09-1989','DD-MM-YYYY'), 'Kolkata', 'West Bengal', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (10, 'Pooja', 'Iyer', 'pooja.iyer@email.com', '9876543219', 'Female', TO_DATE('19-01-1994','DD-MM-YYYY'), 'Chennai', 'Tamil Nadu', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (11, 'Suresh', 'Yadav', 'suresh.yadav@email.com', '9876543220', 'Male', TO_DATE('11-10-1985','DD-MM-YYYY'), 'Lucknow', 'Uttar Pradesh', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (12, 'Anjali', 'Mishra', 'anjali.mishra@email.com', '9876543221', 'Female', TO_DATE('08-02-1997','DD-MM-YYYY'), 'Bhopal', 'Madhya Pradesh', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (13, 'Deepak', 'Joshi', 'deepak.joshi@email.com', '9876543222', 'Male', TO_DATE('27-07-1990','DD-MM-YYYY'), 'Pune', 'Maharashtra', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (14, 'Meera', 'Kulkarni', 'meera.kulkarni@email.com', '9876543223', 'Female', TO_DATE('03-11-1992','DD-MM-YYYY'), 'Nagpur', 'Maharashtra', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (15, 'Karan', 'Malhotra', 'karan.malhotra@email.com', '9876543224', 'Male', TO_DATE('22-05-1986','DD-MM-YYYY'), 'Noida', 'Uttar Pradesh', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (16, 'Divya', 'Saxena', 'divya.saxena@email.com', '9876543225', 'Female', TO_DATE('12-09-1998','DD-MM-YYYY'), 'Indore', 'Madhya Pradesh', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (17, 'Manoj', 'Tiwari', 'manoj.tiwari@email.com', '9876543226', 'Male', TO_DATE('16-03-1984','DD-MM-YYYY'), 'Patna', 'Bihar', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (18, 'Ritu', 'Chopra', 'ritu.chopra@email.com', '9876543227', 'Female', TO_DATE('01-12-1991','DD-MM-YYYY'), 'Ludhiana', 'Punjab', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (19, 'Nitin', 'Agarwal', 'nitin.agarwal@email.com', '9876543228', 'Male', TO_DATE('28-06-1993','DD-MM-YYYY'), 'Kanpur', 'Uttar Pradesh', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO CUSTOMER VALUES (20, 'Shreya', 'Bansal', 'shreya.bansal@email.com', '9876543229', 'Female', TO_DATE('07-04-1995','DD-MM-YYYY'), 'Surat', 'Gujarat', 'India', TO_DATE('20-03-2026','DD-MM-YYYY'))
-- SELECT * FROM DUAL;

-- CREATE TABLE ORDER (
--     ORDER_ID            NUMBER PRIMARY KEY,
--     CUSTOMER_ID         NUMBER NOT NULL,
--     ORDER_DATE          DATE NOT NULL,
--     ORDER_AMOUNT        NUMBER(10,2),
--     ORDER_STATUS        VARCHAR2(20),
--     PAYMENT_METHOD      VARCHAR2(30),
--     SHIPPING_CITY       VARCHAR2(50),
--     SHIPPING_STATE      VARCHAR2(50),
--     SHIPPING_COUNTRY    VARCHAR2(50),
--     CREATED_AT          DATE
-- );

-- INSERT ALL
-- INTO ORDER VALUES (101, 1, TO_DATE('05-01-2026', 'DD-MM-YYYY'), 2500, 'Delivered','Credit Card','Mumbai','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (102, 2, TO_DATE('06-01-2026', 'DD-MM-YYYY'), 1800.50, 'Shipped','UPI','Delhi','Delhi','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (103, 3, TO_DATE('07-01-2026', 'DD-MM-YYYY'), 3200.75, 'Processing','Debit Card','Bangalore','Karnataka','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (104, 4, TO_DATE('08-01-2026', 'DD-MM-YYYY'), 950, 'Cancelled','Cash on Delivery','Ahmedabad','Gujarat','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (105, 5, TO_DATE('09-01-2026', 'DD-MM-YYYY'), 4100.20, 'Delivered','Net Banking','Hyderabad','Telangana','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (106, 6, TO_DATE('10-01-2026', 'DD-MM-YYYY'), 2200, 'Shipped','UPI','Kochi','Kerala','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (107, 7, TO_DATE('11-01-2026', 'DD-MM-YYYY'), 1450.99, 'Delivered','Credit Card','Jaipur','Rajasthan','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (108, 8, TO_DATE('12-01-2026', 'DD-MM-YYYY'), 780, 'Returned','Debit Card','Chandigarh','Punjab','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (109, 9, TO_DATE('13-01-2026', 'DD-MM-YYYY'), 5600, 'Delivered','UPI','Kolkata','West Bengal','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (110, 10, TO_DATE('14-01-2026', 'DD-MM-YYYY'), 1300, 'Processing','Cash on Delivery','Chennai','Tamil Nadu','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (111, 11, TO_DATE('15-01-2026', 'DD-MM-YYYY'), 2750.40, 'Delivered','Net Banking','Lucknow','Uttar Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (112, 12, TO_DATE('16-01-2026', 'DD-MM-YYYY'), 890, 'Cancelled','UPI','Bhopal','Madhya Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (113, 13, TO_DATE('17-01-2026', 'DD-MM-YYYY'), 1999.99, 'Shipped','Credit Card','Pune','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (114, 14, TO_DATE('18-01-2026', 'DD-MM-YYYY'), 3400, 'Delivered','Debit Card','Nagpur','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (115, 15, TO_DATE('19-01-2026', 'DD-MM-YYYY'), 1500.75, 'Returned','Cash on Delivery','Noida','Uttar Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (116, 16, TO_DATE('20-01-2026', 'DD-MM-YYYY'), 2890.30, 'Processing','Net Banking','Indore','Madhya Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (117, 17, TO_DATE('21-01-2026', 'DD-MM-YYYY'), 4700, 'Delivered','Credit Card','Patna','Bihar','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (118, 18, TO_DATE('22-01-2026', 'DD-MM-YYYY'), 999.99, 'Shipped','UPI','Ludhiana','Punjab','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (119, 19, TO_DATE('23-01-2026', 'DD-MM-YYYY'), 2100, 'Delivered','Debit Card','Kanpur','Uttar Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (120, 20, TO_DATE('24-01-2026', 'DD-MM-YYYY'), 3650.60, 'Processing','Cash on Delivery','Surat','Gujarat','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (121, 1, TO_DATE('25-01-2026', 'DD-MM-YYYY'), 1250, 'Delivered','UPI','Mumbai','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (122, 2, TO_DATE('26-01-2026', 'DD-MM-YYYY'), 2150.50, 'Shipped','Credit Card','Delhi','Delhi','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (123, 3, TO_DATE('27-01-2026', 'DD-MM-YYYY'), 980, 'Processing','Debit Card','Bangalore','Karnataka','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (124, 4, TO_DATE('28-01-2026', 'DD-MM-YYYY'), 3050.75, 'Delivered','Net Banking','Ahmedabad','Gujarat','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (125, 5, TO_DATE('29-01-2026', 'DD-MM-YYYY'), 4500, 'Cancelled','Cash on Delivery','Hyderabad','Telangana','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (126, 6, TO_DATE('30-01-2026', 'DD-MM-YYYY'), 1675.25, 'Returned','UPI','Kochi','Kerala','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (127, 7, TO_DATE('31-01-2026', 'DD-MM-YYYY'), 2899.99, 'Delivered','Credit Card','Jaipur','Rajasthan','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (128, 8, TO_DATE('01-02-2026', 'DD-MM-YYYY'), 740, 'Processing','Debit Card','Chandigarh','Punjab','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (129, 9, TO_DATE('02-02-2026', 'DD-MM-YYYY'), 5120.80, 'Delivered','UPI','Kolkata','West Bengal','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (130, 10, TO_DATE('03-02-2026', 'DD-MM-YYYY'), 1320, 'Shipped','Cash on Delivery','Chennai','Tamil Nadu','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (131, 11, TO_DATE('04-02-2026', 'DD-MM-YYYY'), 2600.40, 'Delivered','Net Banking','Lucknow','Uttar Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (132, 12, TO_DATE('05-02-2026', 'DD-MM-YYYY'), 890.90, 'Cancelled','UPI','Bhopal','Madhya Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (133, 13, TO_DATE('06-02-2026', 'DD-MM-YYYY'), 1995, 'Processing','Credit Card','Pune','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (134, 14, TO_DATE('07-02-2026', 'DD-MM-YYYY'), 3750, 'Delivered','Debit Card','Nagpur','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (135, 15, TO_DATE('08-02-2026', 'DD-MM-YYYY'), 1500, 'Returned','Cash on Delivery','Noida','Uttar Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (136, 16, TO_DATE('09-02-2026', 'DD-MM-YYYY'), 2780, 'Shipped','Net Banking','Indore','Madhya Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (137, 17, TO_DATE('10-02-2026', 'DD-MM-YYYY'), 4650.50, 'Delivered','Credit Card','Patna','Bihar','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (138, 18, TO_DATE('11-02-2026', 'DD-MM-YYYY'), 1199.99, 'Processing','UPI','Ludhiana','Punjab','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (139, 19, TO_DATE('12-02-2026', 'DD-MM-YYYY'), 2250, 'Delivered','Debit Card','Kanpur','Uttar Pradesh','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (140, 20, TO_DATE('13-02-2026', 'DD-MM-YYYY'), 3540.60, 'Shipped','Cash on Delivery','Surat','Gujarat','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (141, 1, TO_DATE('14-02-2026', 'DD-MM-YYYY'), 890, 'Delivered','UPI','Mumbai','Maharashtra','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (142, 2, TO_DATE('15-02-2026', 'DD-MM-YYYY'), 2450, 'Processing','Credit Card','Delhi','Delhi','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (143, 3, TO_DATE('16-02-2026', 'DD-MM-YYYY'), 1100, 'Cancelled','Debit Card','Bangalore','Karnataka','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (144, 4, TO_DATE('17-02-2026', 'DD-MM-YYYY'), 3200, 'Delivered','Net Banking','Ahmedabad','Gujarat','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (145, 5, TO_DATE('18-02-2026', 'DD-MM-YYYY'), 4700.25, 'Shipped','Cash on Delivery','Hyderabad','Telangana','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (146, 6, TO_DATE('19-02-2026', 'DD-MM-YYYY'), 1560.75, 'Returned','UPI','Kochi','Kerala','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (147, 7, TO_DATE('20-02-2026', 'DD-MM-YYYY'), 2999.99, 'Delivered','Credit Card','Jaipur','Rajasthan','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (148, 8, TO_DATE('21-02-2026', 'DD-MM-YYYY'), 650, 'Processing','Debit Card','Chandigarh','Punjab','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (149, 9, TO_DATE('22-02-2026', 'DD-MM-YYYY'), 5400, 'Delivered','UPI','Kolkata','West Bengal','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- INTO ORDER VALUES (150, 10, TO_DATE('23-02-2026', 'DD-MM-YYYY'), 1425, 'Shipped','Cash on Delivery','Chennai','Tamil Nadu','India',TO_DATE('20-03-2026','DD-MM-YYYY'))
-- SELECT * FROM DUAL;

-- COMMIT;

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