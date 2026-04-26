from pyspark.sql import SparkSession

spark = (
        SparkSession.builder
        .appName("Hudi LakehouseApp")
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
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .enableHiveSupport()
        .getOrCreate()
    )

source_path = 's3a://shivchoudhury-datasets/MergeSchema/paquet-customers/'

df = spark.read.parquet(source_path)

df.show()

spark.sql("CREATE DATABASE IF NOT EXISTS hudi_db")
spark.sql("""
CREATE TABLE hudi_db.customer_hudi (
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
USING HUDI
TBLPROPERTIES (
    primaryKey = 'CUSTOMER_ID',
    preCombineField = 'BIRTH_YEAR',
    type = 'cow'
)
""")

df.write.format("hudi") \
    .option("hoodie.table.name", "customer_hudi") \
    .option("hoodie.datasource.write.recordkey.field", "CUSTOMER_ID") \
    .option("hoodie.datasource.write.precombine.field", "BIRTH_YEAR") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .mode("overwrite") \
    .save("s3a://shivchoudhury-datasets/warehouse/hudi_db/customer_hudi/")