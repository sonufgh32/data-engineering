-- =========================================================
-- SNOWFLAKE SQL: CTE & RECURSIVE CTE COMPLETE DEMO
-- =========================================================

-- =========================================================
-- 1. CREATE DATABASE & SCHEMA
-- =========================================================

USE ROLE DATA_ENGINEER_ROLE;
USE WAREHOUSE DATA_ENGINEER_WH;

CREATE OR REPLACE DATABASE CTE_DEMO_DB;
USE DATABASE CTE_DEMO_DB;
CREATE OR REPLACE SCHEMA DEMO_SCHEMA;

USE SCHEMA DEMO_SCHEMA;

-- =========================================================
-- 2. EMPLOYEES TABLE
-- =========================================================

CREATE OR REPLACE TABLE employees (
    employee_id NUMBER,
    employee_name STRING,
    manager_id NUMBER,
    department STRING,
    salary NUMBER
);

INSERT INTO employees VALUES
    (1, 'CEO', NULL, 'Management', 250000),
    (2, 'Manager A', 1, 'Engineering', 150000),
    (3, 'Manager B', 1, 'Sales', 145000),
    (4, 'Developer A', 2, 'Engineering', 110000),
    (5, 'Developer B', 2, 'Engineering', 105000),
    (6, 'Sales Rep A', 3, 'Sales', 90000),
    (7, 'Sales Rep B', 3, 'Sales', 92000);

-- =========================================================
-- 3. SALES TABLE
-- =========================================================

CREATE OR REPLACE TABLE sales (
    sale_id NUMBER,
    employee_id NUMBER,
    amount NUMBER,
    sale_date DATE
);

INSERT INTO sales VALUES
    (1, 6, 10000, '2026-01-05'),
    (2, 6, 12000, '2026-01-12'),
    (3, 7, 15000, '2026-01-15'),
    (4, 4, 8000, '2026-02-01'),
    (5, 5, 9000, '2026-02-08'),
    (6, 7, 20000, '2026-02-10'),
    (7, 6, 17000, '2026-03-01'),
    (8, 4, 14000, '2026-03-12');

-- =========================================================
-- 4. CUSTOMERS TABLE
-- =========================================================

CREATE OR REPLACE TABLE customers (
    customer_id NUMBER,
    customer_name STRING,
    city STRING
);

INSERT INTO customers VALUES
    (1, 'John', 'Hyderabad'),
    (2, 'Alice', 'Bangalore'),
    (3, 'Bob', 'Chennai');

-- =========================================================
-- 5. ORDERS TABLE
-- =========================================================

CREATE OR REPLACE TABLE orders (
    order_id NUMBER,
    customer_id NUMBER,
    order_amount NUMBER,
    order_date DATE
);

INSERT INTO orders VALUES
    (101, 1, 5000, '2026-01-10'),
    (102, 1, 7000, '2026-01-15'),
    (103, 2, 4000, '2026-02-01'),
    (104, 3, 9000, '2026-02-12'),
    (105, 2, 6000, '2026-03-05');

-- =========================================================
-- 6. FOLDERS TABLE
-- =========================================================

CREATE OR REPLACE TABLE folders (
    folder_id NUMBER,
    folder_name STRING,
    parent_folder_id NUMBER
);

INSERT INTO folders VALUES
    (1, 'root', NULL),
    (2, 'documents', 1),
    (3, 'photos', 1),
    (4, 'work', 2),
    (5, 'personal', 2),
    (6, 'vacation', 3);

-- =========================================================
-- 7. COMPONENTS TABLE (BILL OF MATERIALS)
-- =========================================================

CREATE OR REPLACE TABLE components (
    part_id STRING,
    component_id STRING,
    quantity NUMBER
);

INSERT INTO components VALUES
    ('BIKE', 'FRAME', 1),
    ('BIKE', 'WHEEL', 2),
    ('BIKE', 'HANDLE', 1),
    ('WHEEL', 'TYRE', 1),
    ('WHEEL', 'RIM', 1);

-- =========================================================
-- =========================================================
-- SIMPLE CTE EXAMPLES
-- =========================================================
-- =========================================================

-- =========================================================
-- EXAMPLE 1: TOTAL SALES BY EMPLOYEE
-- =========================================================

WITH employee_sales AS (
    SELECT
        employee_id,
        SUM(amount) AS total_sales
    FROM sales
    GROUP BY employee_id

)
SELECT
    *
FROM employee_sales
ORDER BY total_sales DESC;

-- =========================================================
-- EXAMPLE 2: MULTIPLE CTEs
-- =========================================================

WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('MONTH', sale_date) AS sales_month,
        SUM(amount) AS revenue
    FROM sales
    GROUP BY 1
),
growth_calc AS (
    SELECT
        sales_month,
        revenue,
        LAG(revenue) OVER (
            ORDER BY sales_month
        ) AS previous_month_revenue
    FROM monthly_sales
)
SELECT
    sales_month,
    revenue,
    previous_month_revenue,
    ROUND(
        (
            (revenue - previous_month_revenue)
            / previous_month_revenue
        ) * 100,
        2
    ) AS growth_percentage
FROM growth_calc;

-- =========================================================
-- EXAMPLE 3: CTE WITH JOINS
-- =========================================================

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_name,
        COUNT(o.order_id) AS total_orders,
        SUM(o.order_amount) AS total_amount
    FROM customers c
    JOIN orders o
        ON c.customer_id = o.customer_id
    GROUP BY
        c.customer_id,
        c.customer_name
)
SELECT
    *
FROM customer_orders
ORDER BY total_amount DESC;

-- =========================================================
-- =========================================================
-- RECURSIVE CTE EXAMPLES
-- =========================================================
-- =========================================================

-- =========================================================
-- EXAMPLE 4: EMPLOYEE HIERARCHY
-- =========================================================

WITH RECURSIVE employee_hierarchy AS (
    -- Anchor Query
    SELECT
        employee_id,
        employee_name,
        manager_id,
        department,
        1 AS level,
        employee_name AS hierarchy_path

    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive Query
    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        e.department,
        eh.level + 1,

        eh.hierarchy_path
            || ' -> '
            || e.employee_name

    FROM employees e
    JOIN employee_hierarchy eh
        ON e.manager_id = eh.employee_id
)
SELECT
    *
FROM employee_hierarchy
ORDER BY level, employee_id;

-- =========================================================
-- EXAMPLE 5: GENERATE NUMBERS
-- =========================================================

WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM numbers
    WHERE n < 10

)
SELECT * FROM numbers;

-- =========================================================
-- EXAMPLE 6: GENERATE DATE SERIES
-- =========================================================

WITH RECURSIVE date_series AS (
    SELECT CURRENT_DATE() AS dt

    UNION ALL
    
    SELECT
        DATEADD(DAY, 1, dt)
    FROM
        date_series
    WHERE
        dt < CURRENT_DATE() + 7
)
SELECT * FROM date_series;

-- =========================================================
-- EXAMPLE 7: FOLDER HIERARCHY
-- =========================================================

WITH RECURSIVE folder_tree AS (
    -- Root folder
    SELECT
        folder_id,
        folder_name,
        parent_folder_id,
        folder_name AS full_path,
        1 AS level
    FROM folders
    WHERE parent_folder_id IS NULL

    UNION ALL

    -- Child folders
    SELECT
        f.folder_id,
        f.folder_name,
        f.parent_folder_id,

        ft.full_path
            || '/'
            || f.folder_name,

        ft.level + 1
    FROM folders f
    JOIN folder_tree ft
        ON f.parent_folder_id = ft.folder_id
)
SELECT
    *
FROM folder_tree
ORDER BY level, folder_id;

-- =========================================================
-- EXAMPLE 8: BILL OF MATERIALS (BOM)
-- =========================================================

WITH RECURSIVE bom AS (
    -- Anchor Query
    SELECT
        part_id,
        component_id,
        quantity,
        1 AS level
    FROM components
    WHERE part_id = 'BIKE'

    UNION ALL

    -- Recursive Query
    SELECT
        c.part_id,
        c.component_id,
        c.quantity,
        b.level + 1
    FROM components c
    JOIN bom b
        ON c.part_id = b.component_id
)
SELECT * FROM bom;

-- =========================================================
-- EXAMPLE 9: FIND TOTAL SUBORDINATES
-- =========================================================

WITH RECURSIVE subordinate_tree AS (
    SELECT
        employee_id,
        employee_name,
        manager_id,
        employee_id AS root_manager
    FROM employees

    UNION ALL

    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        st.root_manager
    FROM employees e
    JOIN subordinate_tree st
        ON e.manager_id = st.employee_id
)

SELECT
    root_manager,
    COUNT(*) - 1 AS total_subordinates
FROM subordinate_tree
GROUP BY root_manager
ORDER BY root_manager;

-- =========================================================
-- EXAMPLE 10: RECURSIVE PATH TRAVERSAL
-- =========================================================

WITH RECURSIVE org_path AS (
    SELECT
        employee_id,
        employee_name,
        manager_id,
        employee_name AS full_path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,

        op.full_path
            || ' => '
            || e.employee_name
    FROM employees e
    JOIN org_path op
        ON e.manager_id = op.employee_id

)
SELECT * FROM org_path;

-- =========================================================
-- CLEANUP COMMANDS (OPTIONAL)
-- =========================================================

DROP DATABASE CTE_DEMO_DB;