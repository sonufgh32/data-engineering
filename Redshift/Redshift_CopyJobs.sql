-- Redshift Copy Job: Create Customers Table
CREATE TABLE public.customers(
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

-- Redshift Copy Job: Create Items Table
CREATE TABLE public.items(
    ITEM_ID VARCHAR,
    ITEM_DESC VARCHAR,
    START_DATE VARCHAR,
    END_DATE VARCHAR,
    PRICE VARCHAR,
    ITEM_CLASS VARCHAR,
    ITEM_CATEGORY VARCHAR
);

-- Redshift Copy Job: Create Orders Table
CREATE TABLE public.orders(
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

-- Load data into Customers table from S3
COPY public.items
FROM 's3://shivchoudhury-datasets/delta/items/'
IAM_ROLE 'arn:aws:iam::741448939728:role/redshift-s3-auto-job-access'
delimiter ','
IGNOREHEADER 1
region 'ap-south-1'
JOB CREATE redshift_copy_s3_items
AUTO ON;

-- Load data into Orders table from S3
COPY public.customers
FROM 's3://shivchoudhury-datasets/delta/customers/'
IAM_ROLE 'arn:aws:iam::741448939728:role/redshift-s3-auto-job-access'
delimiter ','
IGNOREHEADER 1
region 'ap-south-1'
JOB CREATE redshift_copy_s3_customers
AUTO ON;

-- Load data into Orders table from S3
COPY public.orders
FROM 's3://shivchoudhury-datasets/delta/orders/'
IAM_ROLE 'arn:aws:iam::741448939728:role/redshift-s3-auto-job-access'
delimiter ','
IGNOREHEADER 1
region 'ap-south-1'
JOB CREATE redshift_copy_s3_orders
AUTO ON;


-- Verify data load by counting records in each table
SELECT COUNT(*) FROM public.customers;
SELECT COUNT(*) FROM public.items;
SELECT COUNT(*) FROM public.orders;

-- Query Copy Job Metadata
SELECT * FROM SYS_COPY_JOB;

SELECT
    JOB_ID,
    JOB_NAME,
    DATA_SOURCE,
    COPY_QUEUE,
    FILENAME,
    STATUS,
    CURTIME
FROM
    SYS_COPY_JOB COPYJOB
JOIN STL_LOAD_COMMITS LOADCOMMIT
    ON COPYJOB.JOB_ID = LOADCOMMIT.COPY_JOB_ID
WHERE JOB_ID = 106445;

SELECT * FROM SYS_LOAD_HISTORY;

-- can run the job manually when AUTO OFF is configured
COPY JOB RUN redshift_copy_s3_orders;