#!/bin/bash
set -euo pipefail

echo "========================================="
echo "Installing Kafka dependencies"
echo "========================================="

SPARK_JARS_DIR="/home/hadoop/spark-jars"

mkdir -p "$SPARK_JARS_DIR"

cd "$SPARK_JARS_DIR"

echo "Downloading Spark Kafka Connector..."

curl --fail --location --retry 3 --output spark-sql-kafka-0-10_2.12-3.5.6.jar \
	https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar

echo "Downloading Kafka Clients..."

curl --fail --location --retry 3 --output kafka-clients-3.7.0.jar \
	https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar

echo "Downloading AWS MSK IAM Authentication..."

curl --fail --location --retry 3 --output aws-msk-iam-auth.jar \
	https://github.com/aws/aws-msk-iam-auth/releases/download/v2.3.7/aws-msk-iam-auth-2.3.7-all.jar

echo "Bootstrap completed successfully."