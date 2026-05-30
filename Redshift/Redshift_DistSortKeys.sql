-- Distribution Keys
-- 1. DISTKEY	  Large tables that join on the same column	    Column has few distinct values
-- 2. EVEN	      Fact tables with no join pattern	            N/A (safe default)
-- 3. ALL	      Small dimension tables	                    Large tables
-- 4. AUTO        Let Redshift decide                           Recommended

-- Note: SVV :: System View Virtual

-- Distribution Style: Key
CREATE TABLE sales (
    sale_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    amount DECIMAL(10,2),
    sale_date DATE
)
DISTSTYLE KEY
DISTKEY(customer_id);

-- Distribution Style: Even
CREATE TABLE employee_data (
    emp_id BIGINT,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2)
)
DISTSTYLE EVEN;

-- Distribution Style: All
CREATE TABLE dim_product (
    product_id INT,
    product_name VARCHAR(200),
    category VARCHAR(100)
)
DISTSTYLE ALL;

-- Distribution Style: Auto
CREATE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10,2),
    order_date DATE
)
DISTSTYLE AUTO;

-- Verify the distribution key of any table
SELECT table, diststyle FROM SVV_TABLE_INFO WHERE table = 'orders';


-- Sort Keys
-- 1. Single Sort Key :: One column determines sorting.
CREATE TABLE orders (
    order_id bigint,
    order_date timestamp SORTKEY,
    amount decimal(10,2)
);

-- 2. Compound Sort Key :: Sorts by first column, then second.
CREATE TABLE events (
    user_id bigint,
    event_time timestamp,
    product_id int,
    SORTKEY (user_id, event_time)
);

-- 3. Interleaved Sort Key :: Redshift distributes equal weight to all sort columns.
CREATE TABLE sales (
    region varchar(20),
    product_id int,
    sale_date date,
    INTERLEAVED SORTKEY (region, product_id, sale_date)
);

-- Check type of sort keys
SELECT
    table,
    sortkey1,
    sortkey_num,
    sortkey_type
FROM svv_table_info
WHERE table = 'your_table_name';