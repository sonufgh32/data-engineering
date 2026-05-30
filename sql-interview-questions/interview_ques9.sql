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