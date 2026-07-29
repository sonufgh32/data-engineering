import os


AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

TOPIC = os.getenv("TOPIC", "sample-topic")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "")

DEFAULT_NUM_PARTITIONS = 1

DEFAULT_REPLICATION_FACTOR = 3

INTERVAL = int(os.getenv("INTERVAL", "2"))