# ============================================================
# Dockerfile : PySpark + Jupyter + Delta + Iceberg + Hudi
# Glue Catalog + S3 Support
# ============================================================

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Kolkata

# ------------------------------------------------------------
# 1. Install OS Packages
# ------------------------------------------------------------
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    python3-venv \
    python3-full \
    wget \
    curl \
    unzip \
    vim \
    git \
    iputils-ping \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# 2. Install AWS CLI
# ------------------------------------------------------------
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip

# ------------------------------------------------------------
# 3. Environment Variables
# ------------------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# ------------------------------------------------------------
# 4. Install Spark 3.5.1 & Alloy
# ------------------------------------------------------------
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz && \
    cd /usr/local/bin && \
    curl -O -L "https://github.com/grafana/alloy/releases/download/v1.11.3/alloy-linux-amd64.zip" && \
    unzip "alloy-linux-amd64.zip" && \
    rm alloy-linux-amd64.zip && \
    chmod +x alloy-linux-amd64

RUN pwd
RUN ls -l
RUN addgroup --system alloy && adduser --system --ingroup alloy alloy

COPY config.alloy /etc/alloy/config.alloy
COPY entrypoint.sh /entrypoint.sh
COPY log4j.properties /opt/spark/conf/log4j.properties
RUN chmod +x /entrypoint.sh

RUN mkdir -p /etc/alloy

# ------------------------------------------------------------
# 5. Python Virtual Environment
# ------------------------------------------------------------
RUN python3 -m venv /env

ENV PATH="/env/bin:$PATH"

# ------------------------------------------------------------
# 6. Install Python Packages
# ------------------------------------------------------------
RUN pip install --upgrade pip && \
    pip install \
    pyspark==3.5.1 \
    findspark \
    jupyterlab \
    boto3 \
    pandas \
    numpy \
    pyarrow \
    awswrangler \
    sqlalchemy \
    requests \
    pymysql \
    psycopg2-binary \
    pyodbc \
    scikit-learn \
    scipy \
    delta-spark

# ------------------------------------------------------------
# 7. Spark / AWS Runtime Variables
# ------------------------------------------------------------
ENV AWS_DEFAULT_REGION=ap-south-1

ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,\
software.amazon.awssdk:glue:2.25.26,\
software.amazon.awssdk:sts:2.25.26,\
software.amazon.awssdk:s3:2.25.26,\
software.amazon.awssdk:url-connection-client:2.25.26,\
software.amazon.awssdk:dynamodb:2.25.26,\
org.apache.iceberg:iceberg-aws-bundle:1.5.0 pyspark-shell"

# ------------------------------------------------------------
# 8. Jupyter Port
# ------------------------------------------------------------
EXPOSE 8888
EXPOSE 4040
EXPOSE 18080
EXPOSE 12345

WORKDIR /workspace

# ------------------------------------------------------------
# 9. Start Jupyter Lab
# ------------------------------------------------------------
CMD ["/entrypoint.sh", "jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]