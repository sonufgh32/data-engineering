from pyspark.sql import SparkSession

spark = (SparkSession.builder
            .appName("Iceberg LakehouseApp")
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0," \
                                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0," \
                                            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0," \
                                            "org.apache.hadoop.fs.s3a.S3AFileSystem," \
                                            "org.apache.hadoop:hadoop-aws:3.3.4," \
                                            "com.amazonaws:aws-java-sdk-bundle:1.12.262," \
                                            "software.amazon.awssdk:glue:2.25.26," \
                                            "software.amazon.awssdk:sts:2.25.26," \
                                            "software.amazon.awssdk:s3:2.25.26," \
                                            "software.amazon.awssdk:dynamodb:2.25.26," \
                                            "org.apache.iceberg:iceberg-aws-bundle:1.5.0," \
                                            "software.amazon.awssdk:url-connection-client:2.25.26")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.glue_catalog",  "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.warehouse", "s3://shivchoudhury-datasets/warehouse/")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
            .config("spark.sql.catalog.glue_catalog.client.region", "ap-south-1")
            .config("spark.sql.defaultCatalog", "spark_catalog")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .enableHiveSupport()
            .getOrCreate()
        )

source_path = 's3://shivchoudhury-datasets/MergeSchema/paquet-customers/'

df = spark.read.parquet(source_path)

df.show()

spark.sql("CREATE DATABASE IF NOT EXISTS spark_tuts2")

spark.sql("""
CREATE TABLE IF NOT EXISTS spark_tuts2.customer (
    CUSTOMER_ID STRING,
    SALUTATION STRING,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    BIRTH_DAY INT,
    BIRTH_MONTH INT,
    BIRTH_YEAR INT,
    BIRTH_COUNTRY STRING,
    EMAIL_ADDRESS STRING
)
USING iceberg
""")

df.write.format("iceberg").mode("append").save("spark_tuts2.customer")