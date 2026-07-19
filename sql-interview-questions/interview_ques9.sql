-- ROLLUP Example: Total sales at city, state, and country levels
SELECT
    country,
    state,
    city,
    SUM(sales) AS total_sales
FROM sales_data
GROUP BY ROLLUP (country, state, city)
ORDER BY country, state, city;

-- CUBE Example: Total sales for all combinations of country and state
SELECT
    country,
    state,
    city,
    SUM(sales) AS total_sales
FROM sales_data
GROUP BY CUBE (country, state, city)
ORDER BY country, state, city;

-- GROUPING SETS Example: Total sales for specific combinations
SELECT
    country,
    state,
    city,
    SUM(sales) AS total_sales
FROM sales_data
GROUP BY GROUPING SETS (
    (country, state, city),  -- full detail
    (country, state),        -- state totals
    (country),               -- country totals
    ()                       -- grand total
)
ORDER BY country, state, city;

-- Reverse productid within each category

SELECT * FROM PRODUCTS;

WITH REVERSED_PRODUCTS AS (
    SELECT 
        productid,
        product,
        category,
        ROW_NUMBER() OVER (
            PARTITION BY CATEGORY ORDER BY productid DESC) AS RN,
        ROW_NUMBER() OVER (
            PARTITION BY CATEGORY ORDER BY productid) AS RN_REV
    FROM products
)
SELECT
    R1.PRODUCTID,
    R2.PRODUCT,
    R2.CATEGORY
FROM REVERSED_PRODUCTS R1 JOIN REVERSED_PRODUCTS R2
ON R1.CATEGORY = R2.CATEGORY AND R1.RN = R2.RN_REV;


-- Get most frequently visited floor per employee along with total visits and all resources used

SELECT * FROM employee_resources;

with floor_visits as (
    SELECT
        name,
        floor,
        COUNT(*) AS visit_count,
        ROW_NUMBER() OVER (
            PARTITION BY name ORDER BY COUNT(*) DESC) AS RN
    FROM employee_resources
    GROUP BY name, floor
), add_value as (
    SELECT
        name,
        LISTAGG(DISTINCT resources, ', ') WITHIN GROUP (
            ORDER BY resources) AS all_resources
    FROM employee_resources
    GROUP BY name
)
SELECT
    FV.name,
    FV.floor,
    (SELECT COUNT(*) FROM employee_resources ER 
        WHERE ER.name = FV.name) AS total_visits,
    ad.all_resources as resources_used
FROM floor_visits FV JOIN add_value ad on ad.name = FV.name
WHERE FV.RN = 1;

-- Fill down the CATEGORY column where NULLs are present
SELECT * FROM product_brands;

-- Method 1: Using LAG with IGNORE NULLS
WITH tmp as(
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN,
        CATEGORY,
        brand_name
    FROM PRODUCT_BRANDS
)
SELECT
    COALESCE(CATEGORY,
        LAG(CATEGORY IGNORE NULLS) OVER(ORDER BY RN)) AS CATEGORY,
    brand_name
FROM tmp;

-- Method 2: Using FIRST_VALUE with WINDOW FUNCTIONS
WITH tmp as(
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN,
        CATEGORY,
        brand_name
    FROM PRODUCT_BRANDS
), groups as(
    SELECT
        CATEGORY,
        brand_name,
        RN,
        count(CATEGORY) OVER (ORDER BY RN) AS CATEGORY_COUNT
    FROM tmp
)
SELECT
    CATEGORY,
    brand_name,
    FIRST_VALUE(CATEGORY) OVER (
        PARTITION BY CATEGORY_COUNT ORDER BY RN) AS FILLED_CATEGORY
FROM groups;

-- Find lengths of all consecutive 1s sequences in status_log
SELECT * FROM status_log;

with tmp as (
    SELECT
        STATUS,
        ROW_NUMBER() OVER (
            ORDER BY (SELECT NULL)) AS RN
    FROM status_log
), tmp_final as (
    SELECT
        RN,
        STATUS,
        CASE WHEN STATUS = 0 THEN 1 ELSE 0 END AS is_zero,
        SUM(CASE WHEN STATUS = 0 THEN 1 ELSE 0 END) OVER (
            ORDER BY RN) AS zero_count
    FROM tmp
)
SELECT
    SUM(STATUS) AS STATUS_CONSEUTIVE_ONES
FROM tmp_final
GROUP BY zero_count HAVING SUM(STATUS) > 0;

--
select * from cricket_matches;

with all_teams as (
    SELECT team1 as team FROM cricket_matches
    UNION ALL
    SELECT team2 as team FROM cricket_matches
), team_stats as (
    SELECT
        team AS team_name,
        count(*) AS matches_played
    FROM all_teams
    GROUP BY team
)
SELECT
    TEAM_NAME,
    MATCHES_PLAYED,
    SUM(CASE WHEN winner = TEAM_NAME THEN 1 ELSE 0 END) AS WINS,
    SUM(CASE WHEN result = 'DRAW' THEN 1 ELSE 0 END) AS DRAWS,
    SUM(CASE WHEN winner != TEAM_NAME AND result != 'DRAW' THEN 1 ELSE 0 END) AS LOSSES
FROM
    team_stats join cricket_matches
    on (team_stats.team_name = cricket_matches.team1 or
        team_stats.team_name = cricket_matches.team2)
GROUP BY TEAM_NAME, MATCHES_PLAYED;

--
SELECT * FROM customer_journeys;

WITH journey_bounds AS (
    SELECT
        CUSTOMER_ID,
        MIN(JOURNEY_ORDER) AS FIRST_JOURNEY,
        MAX(JOURNEY_ORDER) AS LAST_JOURNEY
    FROM customer_journeys
    GROUP BY CUSTOMER_ID
)
SELECT
    JB.CUSTOMER_ID,
    CJ1.FROM_CITY AS START_CITY,
    CJ2.TO_CITY AS END_CITY
FROM journey_bounds JB JOIN customer_journeys CJ1
    ON CJ1.CUSTOMER_ID = JB.CUSTOMER_ID
    AND CJ1.JOURNEY_ORDER = JB.FIRST_JOURNEY
    JOIN customer_journeys CJ2
    ON CJ2.CUSTOMER_ID = JB.CUSTOMER_ID
    AND CJ2.JOURNEY_ORDER = JB.LAST_JOURNEY
ORDER BY JB.CUSTOMER_ID;

--
SELECT * FROM BANK_TRANSACTIONS;

SELECT
    ACCOUNT_NO,
    TRANSACTION_ID,
    TRANSACTION_TYPE,
    AMOUNT,
    TRANSACTION_DATE,
    SUM(CASE
            WHEN TRANSACTION_TYPE = 'deposit' THEN AMOUNT
            ELSE -AMOUNT
        END)
        OVER (PARTITION BY ACCOUNT_NO ORDER BY TRANSACTION_DATE,
        TRANSACTION_ID
    ) AS CURRENT_BALANCE
FROM BANK_TRANSACTIONS
WHERE ACCOUNT_NO = '500001';

--
SELECT * FROM travel_routes;

-- Using LEAST and GREATEST (platform-dependent)
SELECT
    DISTINCT
    LEAST(SOURCE, DESTINATION) AS START_CITY,
    GREATEST(SOURCE, DESTINATION) AS END_CITY,
    DISTANCE
FROM travel_routes;

-- Generic SQL approach without platform-dependent functions
SELECT
    DISTINCT
    CASE WHEN SOURCE < DESTINATION THEN SOURCE ELSE DESTINATION END AS START_CITY,
    CASE WHEN SOURCE < DESTINATION THEN DESTINATION ELSE SOURCE END AS END_CITY,
    DISTANCE
FROM travel_routes;

-- Query to get the service which are down from last 3 mins

-- CREATE TABLE service_status (
--     service_name VARCHAR2(30),
--     updated_time TIMESTAMP,
--     status VARCHAR2(10)
-- );

-- INSERT ALL

-- -- HDFS (4 consecutive DOWN)
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:00:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:01:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:02:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:03:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:04:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:05:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hdfs', TO_TIMESTAMP('2024-03-06 10:06:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- YARN (3 consecutive DOWN)
-- INTO service_status VALUES ('yarn', TO_TIMESTAMP('2024-03-06 10:07:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('yarn', TO_TIMESTAMP('2024-03-06 10:08:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('yarn', TO_TIMESTAMP('2024-03-06 10:09:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('yarn', TO_TIMESTAMP('2024-03-06 10:10:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('yarn', TO_TIMESTAMP('2024-03-06 10:11:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- Kafka (5 consecutive DOWN)
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:12:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:13:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:14:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:15:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:16:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:17:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('kafka', TO_TIMESTAMP('2024-03-06 10:18:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- Spark (No failure)
-- INTO service_status VALUES ('spark', TO_TIMESTAMP('2024-03-06 10:19:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('spark', TO_TIMESTAMP('2024-03-06 10:20:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('spark', TO_TIMESTAMP('2024-03-06 10:21:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('spark', TO_TIMESTAMP('2024-03-06 10:22:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- Hive (Two different failure periods)
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:23:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:24:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:25:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:26:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:27:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:28:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:29:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hive', TO_TIMESTAMP('2024-03-06 10:30:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- HBase (3 DOWN)
-- INTO service_status VALUES ('hbase', TO_TIMESTAMP('2024-03-06 10:31:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('hbase', TO_TIMESTAMP('2024-03-06 10:32:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hbase', TO_TIMESTAMP('2024-03-06 10:33:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hbase', TO_TIMESTAMP('2024-03-06 10:34:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('hbase', TO_TIMESTAMP('2024-03-06 10:35:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- Zookeeper (4 DOWN)
-- INTO service_status VALUES ('zookeeper', TO_TIMESTAMP('2024-03-06 10:36:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('zookeeper', TO_TIMESTAMP('2024-03-06 10:37:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('zookeeper', TO_TIMESTAMP('2024-03-06 10:38:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('zookeeper', TO_TIMESTAMP('2024-03-06 10:39:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('zookeeper', TO_TIMESTAMP('2024-03-06 10:40:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('zookeeper', TO_TIMESTAMP('2024-03-06 10:41:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- NiFi (Normal)
-- INTO service_status VALUES ('nifi', TO_TIMESTAMP('2024-03-06 10:42:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('nifi', TO_TIMESTAMP('2024-03-06 10:43:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('nifi', TO_TIMESTAMP('2024-03-06 10:44:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- -- Airflow (3 DOWN)
-- INTO service_status VALUES ('airflow', TO_TIMESTAMP('2024-03-06 10:45:00','YYYY-MM-DD HH24:MI:SS'),'up')
-- INTO service_status VALUES ('airflow', TO_TIMESTAMP('2024-03-06 10:46:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('airflow', TO_TIMESTAMP('2024-03-06 10:47:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('airflow', TO_TIMESTAMP('2024-03-06 10:48:00','YYYY-MM-DD HH24:MI:SS'),'down')
-- INTO service_status VALUES ('airflow', TO_TIMESTAMP('2024-03-06 10:49:00','YYYY-MM-DD HH24:MI:SS'),'up')

-- SELECT * FROM dual;

-- COMMIT;

SELECT * FROM service_status;

SELECT
    SERVICE_NAME,
    UPDATED_TIME,
    STATUS,
    ROW_NUMBER() OVER(PARTITION BY SERVICE_NAME ORDER BY UPDATED_TIME ASC) AS RN,
    RANK() OVER(PARTITION BY SERVICE_NAME, STATUS ORDER BY UPDATED_TIME ASC) AS RNK
FROM
    SERVICE_STATUS WHERE SERVICE_NAME = 'hdfs';

-- Solution using ROW_NUMBER() AND RANK()
WITH TMP AS(
    SELECT
        SERVICE_NAME,
        UPDATED_TIME,
        STATUS,
        ROW_NUMBER() OVER(PARTITION BY SERVICE_NAME ORDER BY UPDATED_TIME ASC) -
            RANK() OVER(PARTITION BY SERVICE_NAME, STATUS ORDER BY UPDATED_TIME ASC) AS GRP
    FROM
        SERVICE_STATUS
),
RES AS(
    SELECT
        SERVICE_NAME,
        MIN(UPDATED_TIME) AS START_TIME,
        MAX(UPDATED_TIME) AS END_TIME,
        STATUS
    FROM
        TMP
    WHERE STATUS = 'down'
    GROUP BY SERVICE_NAME, STATUS
    HAVING COUNT(1) > 3
)
SELECT * FROM RES;

-- Solution : 2
SELECT
    SERVICE_NAME,
    MIN(UPDATED_TIME) AS START_UPDATED_TIME,
    MAX(UPDATED_TIME) AS END_UPDATED_TIME,
    CASE
        WHEN MAX(STATUS) = 'down' THEN 'Y'
        ELSE 'N'
    END AS FLAG
FROM (
    SELECT
        SERVICE_NAME,
        STATUS,
        UPDATED_TIME,
        ROW_NUMBER() OVER (ORDER BY UPDATED_TIME)
        - ROW_NUMBER() OVER (PARTITION BY STATUS ORDER BY UPDATED_TIME) AS GRP
    FROM SERVICE_STATUS
)
GROUP BY SERVICE_NAME, GRP
HAVING MAX(UPDATED_TIME) - MIN(UPDATED_TIME) >= NUMTODSINTERVAL(3, 'MINUTE');

-- Query to groupby on certain values, and show all other column values as list
SELECT * FROM SERVICE_STATUS;

SELECT
    SERVICE_NAME,
    LISTAGG(STATUS, ',')
FROM
    SERVICE_STATUS
GROUP BY SERVICE_NAME;


-- You have an orders tables with order_id, customer_id, billing_address and shipping_address.
-- Find orders with a different billing and shipping addess.
-- CREATE TABLE orders_goog_ques (
--     order_id NUMBER PRIMARY KEY,
--     customer_id NUMBER,
--     billing_address VARCHAR2(100),
--     shipping_address VARCHAR2(100)
-- );

-- -- Customer 1001 (Always same billing & shipping)
-- INSERT INTO orders_goog_ques VALUES (101, 1001, 'Hyderabad', 'Hyderabad');
-- INSERT INTO orders_goog_ques VALUES (102, 1001, 'Hyderabad', 'Hyderabad');
-- INSERT INTO orders_goog_ques VALUES (103, 1001, 'Hyderabad', 'Hyderabad');

-- -- Customer 1002 (Different combinations)
-- INSERT INTO orders_goog_ques VALUES (104, 1002, 'Mumbai', 'Mumbai');
-- INSERT INTO orders_goog_ques VALUES (105, 1002, 'Mumbai', 'Pune');
-- INSERT INTO orders_goog_ques VALUES (106, 1002, 'Delhi', 'Pune');

-- -- Customer 1003 (Same billing, different shipping)
-- INSERT INTO orders_goog_ques VALUES (107, 1003, 'Kolkata', 'Kolkata');
-- INSERT INTO orders_goog_ques VALUES (108, 1003, 'Kolkata', 'Hyderabad');
-- INSERT INTO orders_goog_ques VALUES (109, 1003, 'Kolkata', 'Bangalore');

-- -- Customer 1004 (Always same billing & shipping)
-- INSERT INTO orders_goog_ques VALUES (110, 1004, 'Delhi', 'Delhi');
-- INSERT INTO orders_goog_ques VALUES (111, 1004, 'Delhi', 'Delhi');

-- -- Customer 1005 (Billing changes, shipping same)
-- INSERT INTO orders_goog_ques VALUES (112, 1005, 'Pune', 'Mumbai');
-- INSERT INTO orders_goog_ques VALUES (113, 1005, 'Ahmedabad', 'Mumbai');
-- INSERT INTO orders_goog_ques VALUES (114, 1005, 'Surat', 'Mumbai');

-- -- Customer 1006 (Always same billing & shipping)
-- INSERT INTO orders_goog_ques VALUES (115, 1006, 'Jaipur', 'Jaipur');
-- INSERT INTO orders_goog_ques VALUES (116, 1006, 'Jaipur', 'Jaipur');

-- -- Customer 1007 (Mixed combinations)
-- INSERT INTO orders_goog_ques VALUES (117, 1007, 'Lucknow', 'Kanpur');
-- INSERT INTO orders_goog_ques VALUES (118, 1007, 'Lucknow', 'Lucknow');

-- -- Customer 1008 (Billing and shipping swapped)
-- INSERT INTO orders_goog_ques VALUES (119, 1008, 'Bhopal', 'Indore');
-- INSERT INTO orders_goog_ques VALUES (120, 1008, 'Indore', 'Bhopal');

-- COMMIT;

SELECT * FROM orders_goog_ques;

SELECT
    DISTINCT
    O1.customer_id,
    O1.billing_address,
    O2.shipping_address
FROM
    ORDERS_GOOG_QUES O1
    INNER  JOIN ORDERS_GOOG_QUES O2
    ON O1.CUSTOMER_ID = O2.CUSTOMER_ID AND
    O1.ORDER_ID != O2.ORDER_ID AND
    O1.BILLING_ADDRESS != O2.SHIPPING_ADDRESS;