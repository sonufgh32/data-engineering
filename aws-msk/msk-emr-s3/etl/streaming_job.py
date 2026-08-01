from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from config import (
    APP_NAME,
    CHECKPOINT_PATH_CONFIG,
    MSK_BOOTSTRAP_SERVERS_CONFIG,
    OUTPUT_PATH_CONFIG,
    TOPIC_NAME_CONFIG,
    get_required_spark_config,
)
from schema import transaction_schema
from utils import clean

spark = (
    SparkSession.builder
        .appName(APP_NAME)
        .enableHiveSupport()
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Terraform supplies deployment-specific values through spark-submit --conf flags.
msk_bootstrap_servers = get_required_spark_config(spark, MSK_BOOTSTRAP_SERVERS_CONFIG)
topic_name = get_required_spark_config(spark, TOPIC_NAME_CONFIG)
output_path = get_required_spark_config(spark, OUTPUT_PATH_CONFIG)
checkpoint_path = get_required_spark_config(spark, CHECKPOINT_PATH_CONFIG)

raw_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", msk_bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
    .option(
        "kafka.sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required;",
    )
    .option(
        "kafka.sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    )
    .load()
)

# Kafka payloads are UTF-8 JSON transaction records.
json_df = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
)

parsed = (
    json_df
    .select(from_json(col("value"), transaction_schema).alias("json"))
    .select("json.*")
)

clean_df = clean(parsed)

# Store only validated, de-duplicated transactions. The checkpoint makes offsets and
# sink progress recoverable across EMR step retries.
query = (
    clean_df
    .writeStream
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .start()
)

query.awaitTermination()