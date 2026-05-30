-- Different types of compression encodings in Redshift
-- 1. RAW: No compression
-- 2. LZO: Good for text data, balances compression ratio and speed
-- 3. ZSTD: High compression ratio, good for large datasets
-- 4. BYTEDICT: Effective for low cardinality columns
-- 5. RUNLENGTH: Best for columns with repeated values
-- 6. DELTA: Good for sorted numeric data
-- 7. DELTA32K: Similar to DELTA but for larger ranges of integers

-- Example table with different compression encodings
CREATE TABLE public.compressed_data (
    id INT ENCODE RAW,
    name VARCHAR(100) ENCODE LZO,
    description VARCHAR(255) ENCODE ZSTD,
    category VARCHAR(50) ENCODE BYTEDICT,
    status VARCHAR(20) ENCODE RUNLENGTH,
    value INT ENCODE DELTA,
    large_value BIGINT ENCODE DELTA32K
);

-- To analyze and apply optimal compression encodings based on data
ANALYZE COMPRESSION table_name;

-- Optional: create a representative sample (keeps sort order if relevant)
CREATE TEMP TABLE t_orders_sample AS
SELECT *
FROM orders
ORDER BY order_date
LIMIT 50_000;

-- Analyze compression on the main table and sample table
ANALYZE COMPRESSION orders;
ANALYZE COMPRESSION t_orders_sample;
ANALYZE COMPRESSION orders COLUMN (status, customer_id, amount);

-- Alter table to set specific compression encodings
ALTER TABLE orders
ALTER COLUMN amount ENCODE AZ64,
ALTER COLUMN status ENCODE BYTEDICT,
ALTER COLUMN notes ENCODE ZSTD;

-- (Optional) Vacuum/Analyze afterwards to tidy storage & stats
VACUUM REINDEX orders;
ANALYZE orders;