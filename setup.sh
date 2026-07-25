#!/usr/bin/env bash

set -euo pipefail

# Directory where setup.sh is located
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PACKAGE_DIR="${PROJECT_DIR}/pyspark/packages"
JAR_DIR="${PROJECT_DIR}/pyspark/spark-jars"

mkdir -p "${PACKAGE_DIR}"
mkdir -p "${JAR_DIR}"

# Files to download
URLS=(
  "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz"
  "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
  "https://github.com/prometheus/node_exporter/releases/download/v1.8.2/node_exporter-1.8.2.linux-amd64.tar.gz"
  "https://github.com/grafana/alloy/releases/download/v1.11.3/alloy-linux-amd64.zip"
)

echo "Downloading packages..."

for url in "${URLS[@]}"; do
    file="$(basename "$url")"
    destination="${PACKAGE_DIR}/${file}"

    if [[ -f "$destination" ]]; then
        echo "Already exists: $file"
        continue
    fi

    echo "Downloading $file..."

    if command -v curl >/dev/null 2>&1; then
        curl -L "$url" -o "$destination"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$destination" "$url"
    else
        echo "Error: Neither curl nor wget is installed."
        exit 1
    fi

    echo "Downloaded: $file"
done

echo
echo "All package downloads completed."
echo "Location: ${PACKAGE_DIR}"

echo
echo "Downloading Maven dependencies..."

cd "$PROJECT_DIR"

if command -v mvn >/dev/null 2>&1; then
    mvn dependency:copy-dependencies \
        -DincludeScope=runtime \
        -DoutputDirectory="${JAR_DIR}"
elif [[ -x "./mvnw" ]]; then
    ./mvnw dependency:copy-dependencies \
        -DincludeScope=runtime \
        -DoutputDirectory="${JAR_DIR}"
else
    echo "Error: Maven (mvn) or Maven Wrapper (mvnw) not found."
    exit 1
fi

echo
echo "Maven dependencies downloaded successfully."
echo "Location: ${JAR_DIR}"