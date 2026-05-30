-- Suppose you have two table table A and table B, table B
-- was exact replica of table A and A does not have primary key.
-- A new records comes into B from A which was already present in
-- B Then how to avoid inserting duplicates records in B?

SELECT * FROM CARS_A;
SELECT * FROM CARS_B;

-- Method 1
INSERT INTO CARS_B (BRAND, MODEL, YEAR, COLOR)
SELECT
    A.BRAND,
    A.MODEL,
    A.YEAR,
    A.COLOR
FROM CARS_A A
WHERE NOT EXISTS(
    SELECT
        1
    FROM CARS_B B
    WHERE A.BRAND = B.BRAND
    AND A.MODEL = B.MODEL
    AND A.YEAR = B.YEAR
    AND A.COLOR = B.COLOR 
);

INSERT INTO cars_a VALUES ('Chevrolet', 'Malibu', 2018, 'Gray');
INSERT INTO cars_a VALUES ('BMW', 'X5', 2022, 'Black');
INSERT INTO cars_a VALUES ('Audi', 'A4', 2021, 'White');
INSERT INTO cars_a VALUES ('Kia', 'Seltos', 2020, 'Blue');
INSERT INTO cars_a VALUES ('Nissan', 'Altima', 2019, 'Red');
INSERT INTO cars_a VALUES ('Volkswagen', 'Passat', 2021, 'Silver');
INSERT INTO cars_a VALUES ('Mazda', 'CX-5', 2023, 'Gray');
INSERT INTO cars_a VALUES ('Tesla', 'Model 3', 2022, 'White');
INSERT INTO cars_a VALUES ('Toyota', 'Corolla', 2018, 'Blue');
INSERT INTO cars_a VALUES ('Hyundai', 'Tucson', 2021, 'Red');
INSERT INTO cars_a VALUES ('Honda', 'Accord', 2020, 'Black');
INSERT INTO cars_a VALUES ('Ford', 'Escape', 2019, 'White');
INSERT INTO cars_a VALUES ('Chevrolet', 'Equinox', 2023, 'Blue');
INSERT INTO cars_a VALUES ('BMW', '3 Series', 2020, 'Silver');
INSERT INTO cars_a VALUES ('Kia', 'Sportage', 2022, 'Black');

-- Method 2
MERGE INTO CARS_B B
USING CARS_A A
ON (
    A.BRAND = B.BRAND
    AND A.MODEL = B.MODEL
    AND A.YEAR = B.YEAR
    AND A.COLOR = B.COLOR
)
WHEN NOT MATCHED THEN
    INSERT (BRAND, MODEL, YEAR, COLOR)
    VALUES (A.BRAND, A.MODEL, A.YEAR, A.COLOR);


--

SELECT * FROM ACTIVITY;

-- Method 1
WITH RES AS(
    SELECT
        MACHINE_ID,
        PROCESS_ID,
        SUM(
            CASE
                WHEN ACTIVITY_TYPE = 'start' THEN -TIMESTAMP
                WHEN ACTIVITY_TYPE = 'end' THEN TIMESTAMP
            END
        ) AS TOTAL_TIME
    FROM
        ACTIVITY
    GROUP BY MACHINE_ID, PROCESS_ID
)
SELECT
    MACHINE_ID,
    AVG(TOTAL_TIME) AS PROCESSING_TIME
FROM
    RES
GROUP BY MACHINE_ID;

-- Method 2
SELECT
    A1.MACHINE_ID,
    ROUND(AVG(A2.TIMESTAMP - A1.TIMESTAMP), 3) AS PROCESSING_TIME
FROM
    ACTIVITY A1 INNER JOIN ACTIVITY A2
    ON A1.MACHINE_ID = A2.MACHINE_ID
    AND A1.PROCESS_ID = A2.PROCESS_ID
    AND A1.ACTIVITY_TYPE = 'start'
    AND A2.ACTIVITY_TYPE = 'end'
GROUP BY A1.MACHINE_ID;

-- 
-- Write a SQL query to find the each month total transactions with
-- total amount and total approved transactions with total amount.

SELECT * FROM transactions;

SELECT
    EXTRACT(MONTH FROM transaction_date) AS TXNS_MONTH,
    COUNTRY,
    COUNT(1) AS TOTAL_COUNT,
    SUM(AMOUNT) AS TOTAL_AMOUNT,
    SUM(CASE
        WHEN STATE = 'APPROVED' THEN 1
        ELSE 0
    END) AS TOTAL_APPROVED_COUNT,
    SUM(
        CASE
            WHEN STATE = 'APPROVED' THEN AMOUNT
            ELSE 0
        END
    ) AS TOTAL_APPROVED_AMOUNT,
    SUM(CASE
        WHEN STATE = 'DECLINED' THEN 1
        ELSE 0
    END) AS TOTAL_DECLINED_COUNT,
    SUM(
        CASE
            WHEN STATE = 'DECLINED' THEN AMOUNT
            ELSE 0
        END
    ) AS TOTAL_DECLINED_AMOUNT
FROM transactions
GROUP BY EXTRACT(MONTH FROM transaction_date), COUNTRY;

SELECT
    DISTINCT
    EXTRACT(MONTH FROM transaction_date) AS TXNS_MONTH,
    COUNTRY,
    COUNT(1) OVER(PARTITION BY EXTRACT(MONTH FROM transaction_date), COUNTRY) AS TOTAL_TXNS_COUNT,
    SUM(AMOUNT) OVER(PARTITION BY EXTRACT(MONTH FROM transaction_date), COUNTRY) AS TOTAL_TXNS_AMOUNT,
    COUNT(CASE WHEN STATE = 'APPROVED' THEN 1 ELSE 0 END) OVER(PARTITION BY EXTRACT(MONTH FROM transaction_date), COUNTRY) AS TOTAL_APPROVED_COUNT,
    SUM(CASE WHEN STATE = 'APPROVED' THEN AMOUNT ELSE 0 END) OVER(PARTITION BY EXTRACT(MONTH FROM transaction_date), COUNTRY) AS TOTAL_APPROVED_AMOUNT
FROM transactions;

-- 

SELECT * FROM emp_swipe_log;

SELECT
    EMP_ID,
    SWIPE_TIME,
    status
FROM EMP_SWIPE_LOG
GROUP BY EMP_ID, SWIPE_TIME, status
ORDER BY EMP_ID, SWIPE_TIME DESC;

WITH FINAL AS(
    SELECT
        EMP_ID,
        SWIPE_TIME,
        STATUS,
        ROW_NUMBER() OVER(PARTITION BY EMP_ID ORDER BY EMP_ID, SWIPE_TIME DESC) AS RN
    FROM
        EMP_SWIPE_LOG
)
SELECT
    EMP_ID,
    SWIPE_TIME
FROM FINAL
WHERE
    RN = 1
    AND STATUS = 'IN';

SELECT * FROM consecutive_vals;

WITH RES AS(
    SELECT
        val,
        LEAD(val, 1) OVER(ORDER BY id) as ONE,
        LEAD(val, 2) OVER(ORDER BY id) as TWO
    FROM consecutive_vals
)
SELECT DISTINCT VAL FROM RES WHERE VAL = ONE AND VAL = TWO;

SELECT
    DISTINCT
    curr.val
FROM
    consecutive_vals curr
    INNER JOIN consecutive_vals next ON curr.id = next.id - 1
    INNER JOIN consecutive_vals prev ON curr.id = prev.id + 1
WHERE
    curr.val = next.val
    and curr.val = prev.val;

-- 

SELECT * FROM test_values;

-- Method 1
SELECT
    id,
    val,
    LAST_VALUE(val IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS filled_val
FROM test_values;

-- Method 2
SELECT
    id,
    val,
    COALESCE(val, LAG(val IGNORE NULLS) OVER (ORDER BY id)) AS filled_val
FROM TEST_VALUES;

-- Method 3
WITH fill_cte (id, val, filled_val) AS (
    SELECT
        id,
        val,
        val
    FROM
        test_values
    WHERE id = 1
    UNION ALL
    SELECT
        t.id,
        t.val,
        COALESCE(t.val, f.filled_val)
    FROM
        test_values t
        JOIN fill_cte f ON t.id = f.id + 1
) SELECT * FROM fill_cte;