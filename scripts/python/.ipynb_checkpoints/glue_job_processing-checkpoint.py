import sys
from awsglue.transforms import *
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
import time
from awsglue.dynamicframe import DynamicFrame
# F1VY5UVqXhRhpdXcFEq6


# Create a logger from logging module set date time basic configuration
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.argv += ["--JOB_NAME", "Glue Job Processing"]

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

logger.info("Arguments: %s", args)

spark = (
        SparkSession.builder
        .appName("Glue Job Processing App")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,"
                                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                                        "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
                                        "org.apache.hadoop:hadoop-aws:3.3.4,"
                                        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                                        "software.amazon.awssdk:glue:2.25.26,"
                                        "software.amazon.awssdk:sts:2.25.26,"
                                        "software.amazon.awssdk:s3:2.25.26,"
                                        "software.amazon.awssdk:dynamodb:2.25.26,"
                                        "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
                                        "software.amazon.awssdk:url-connection-client:2.25.26")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,"
                                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                                        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3a://shivchoudhury-datasets/warehouse/")
        .config("spark.sql.warehouse.dir", "s3a://shivchoudhury-datasets/warehouse/")
        .config("spark.sql.catalog.glue_catalog.client.region", "ap-south-1")
        .config("spark.sql.defaultCatalog", "glue_catalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .enableHiveSupport()
        .getOrCreate()
    )

sc = spark.sparkContext
gc = GlueContext(sc)
job = Job(gc)
job.init(args['JOB_NAME'], args)

# Customer Schema: CUSTOMER_ID, SALUTATION, FIRST_NAME, LAST_NAME, BIRTH_DAY, BIRTH_MONTH, BIRTH_YEAR, BIRTH_COUNTRY, EMAIL_ADDRESS
CUSTOMER_PATH = "s3a://shivchoudhury-datasets/customer/"

# Order Schema: ORDER_DATE, ORDER_TIME, ITEM_ID, ITEM_DESC, CUSTOMER_ID	SALUTATION, FIRST_NAME, LAST_NAME, STORE_ID, STORE_NAME, ORDER_QUANTITY, SALE_PRICE, DISCOUNT_AMT, COUPON_AMT, NET_PAID, NET_PAID_TAX, NET_PROFIT
ORDER_PATH = "s3a://shivchoudhury-datasets/order/"

# Item Schema: ITEM_ID, ITEM_DESC, START_DATE, END_DATE, PRICE, ITEM_CLASS, ITEM_CATEGORY
ITEM_PATH = "s3a://shivchoudhury-datasets/item/"

# Use these three paths to read the data and create three dataframes for customer, order and item and do some processing on the dataframes and write the output to s3 in parquet format
customer_df = spark.read.format("csv").option("header", "true").load(CUSTOMER_PATH)
order_df = spark.read.format("csv").option("header", "true").load(ORDER_PATH)
item_df = spark.read.format("csv").option("header", "true").load(ITEM_PATH)

# Join the three dataframes on CUSTOMER_ID and ITEM_ID and select the required columns
joined_df = (order_df
             .join(customer_df, "CUSTOMER_ID", "inner")
             .join(item_df, "ITEM_ID", "inner")
             .select("ORDER_DATE", "ORDER_TIME", "ITEM_ID", "ITEM_DESC", "CUSTOMER_ID",
                     "SALUTATION", "FIRST_NAME", "LAST_NAME", "STORE_ID", "STORE_NAME",
                     "ORDER_QUANTITY", "SALE_PRICE", "DISCOUNT_AMT", "COUPON_AMT",
                     "NET_PAID", "NET_PAID_TAX", "NET_PROFIT")
            )   

# Write the output to s3 in parquet format
OUTPUT_PATH = "s3a://shivchoudhury-datasets/output/"
joined_df.write.mode("overwrite").parquet(OUTPUT_PATH)

# Wait for 10 minutes before committing the job to ensure that the output is written to s3
time.sleep(600)
job.commit()
