-- Create an external schema for demonstration purposes
CREATE SCHEMA EXT_FILE_DEMO;

-- Create an external table for customers data
CREATE TABLE EXT_FILE_DEMO.customers(
    CUSTOMER_ID VARCHAR,
    SALUTATION VARCHAR,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    BIRTH_DAY VARCHAR,
    BIRTH_MONTH VARCHAR,
    BIRTH_YEAR VARCHAR,
    BIRTH_COUNTRY VARCHAR,
    EMAIL_ADDRESS VARCHAR
);

-- Load data into the external table from S3
COPY EXT_FILE_DEMO.customers
FROM 's3://shivchoudhury-datasets/customers/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::741448939728:role/redshift-spectrum-role'
delimiter ','
IGNOREHEADER 1
region 'ap-south-1';

SELECT COUNT(*) FROM EXT_FILE_DEMO.customers;

-- Create an external table for items data
CREATE TABLE EXT_FILE_DEMO.items(
    ITEM_ID VARCHAR,
    ITEM_DESC VARCHAR,
    START_DATE VARCHAR,
    END_DATE VARCHAR,
    PRICE VARCHAR,
    ITEM_CLASS VARCHAR,
    ITEM_CATEGORY VARCHAR
);

COPY EXT_FILE_DEMO.items
FROM 's3://shivchoudhury-datasets/items/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::741448939728:role/redshift-spectrum-role'
delimiter ','
IGNOREHEADER 1
region 'ap-south-1';

SELECT COUNT(*) FROM EXT_FILE_DEMO.items;

-- Create an external table for orders data
CREATE TABLE EXT_FILE_DEMO.orders(
    ORDER_DATE VARCHAR,
    ORDER_TIME VARCHAR,
    ITEM_ID VARCHAR,
    ITEM_DESC VARCHAR,
    CUSTOMER_ID VARCHAR,
    SALUTATION VARCHAR,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    STORE_ID VARCHAR,
    STORE_NAME VARCHAR,
    ORDER_QUANTITY VARCHAR,
    SALE_PRICE VARCHAR,
    DISCOUNT_AMT VARCHAR,
    COUPON_AMT VARCHAR,
    NET_PAID VARCHAR,
    NET_PAID_TAX VARCHAR,
    NET_PROFIT VARCHAR
);

COPY EXT_FILE_DEMO.orders
FROM 's3://shivchoudhury-datasets/orders/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::741448939728:role/redshift-spectrum-role'
delimiter ','
IGNOREHEADER 1
region 'ap-south-1';

SELECT COUNT(*) FROM EXT_FILE_DEMO.orders;

-- unload command is used to load data from redshift to S3
UNLOAD ('SELECT * FROM public.orders')
TO 's3://'
IAM_ROLE '';