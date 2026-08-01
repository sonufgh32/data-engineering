APP_NAME = "MSK Serverless Streaming"

MSK_BOOTSTRAP_SERVERS_CONFIG = "spark.msk-emr-s3.bootstrap-servers"
TOPIC_NAME_CONFIG = "spark.msk-emr-s3.topic-name"
OUTPUT_PATH_CONFIG = "spark.msk-emr-s3.output-path"
CHECKPOINT_PATH_CONFIG = "spark.msk-emr-s3.checkpoint-path"


def get_required_spark_config(spark, key):
    """Return a required spark-submit setting with a clear deployment error."""
    value = spark.conf.get(key, None)
    if not value:
        raise ValueError(f"Missing required Spark configuration: {key}")
    return value