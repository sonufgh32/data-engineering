-- Redshift Spectrum: Create External Schema and Table for Sales Data
CREATE EXTERNAL SCHEMA sales_spectrum
FROM DATA CATALOG
DATABASE 'sales_spectrum'
IAM_ROLE 'arn:aws:iam::741448939728:role/redshift-spectrum-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Create External Table for Customers Data in S3
CREATE EXTERNAL TABLE sales_spectrum.customers(
    CUSTOMER_ID VARCHAR,
    SALUTATION VARCHAR,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    BIRTH_DAY BIGINT,
    BIRTH_MONTH BIGINT,
    BIRTH_YEAR BIGINT,
    BIRTH_COUNTRY VARCHAR,
    EMAIL_ADDRESS VARCHAR
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 's3://shivchoudhury-datasets/customers/'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- Create External Table for Items Data in S3
CREATE EXTERNAL TABLE sales_spectrum.items(
    ITEM_ID VARCHAR,
    ITEM_DESC VARCHAR,
    START_DATE DATE,
    END_DATE DATE,
    PRICE DECIMAL,
    ITEM_CLASS VARCHAR,
    ITEM_CATEGORY VARCHAR
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 's3://shivchoudhury-datasets/items/'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- Create External Table for Orders Data in S3
CREATE EXTERNAL TABLE sales_spectrum.orders(
    ORDER_DATE DATE,
    ORDER_TIME VARCHAR,
    ITEM_ID VARCHAR,
    ITEM_DESC VARCHAR,
    CUSTOMER_ID VARCHAR,
    SALUTATION VARCHAR,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    STORE_ID VARCHAR,
    STORE_NAME VARCHAR,
    ORDER_QUANTITY BIGINT,
    SALE_PRICE DECIMAL,
    DISCOUNT_AMT DECIMAL,
    COUPON_AMT DECIMAL,
    NET_PAID DECIMAL,
    NET_PAID_TAX DECIMAL,
    NET_PROFIT DECIMAL
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 's3://shivchoudhury-datasets/orders/'
TABLE PROPERTIES ('skip.header.line.count'='1');

SELECT count(*) FROM sales_spectrum.orders;
SELECT count(*) FROM sales_spectrum.items;
SELECT count(*) FROM sales_spectrum.customers;