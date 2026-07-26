#!/bin/bash
set -e

if [ "$(id -u)" -ne 0 ]; then
  echo "Re-running as root..."
  exec sudo bash "$0" "$@"
fi

LOG_FILE="/var/log/user-data.log"
if [ "$EUID" -ne 0 ]; then
  LOG_FILE="$HOME/user-data.log"
fi

exec > >(tee -a "$LOG_FILE") 2>&1

echo "Updating system..."
dnf update -y

echo "Installing Java..."
dnf install -y java-17-amazon-corretto

echo "Installing tools..."
dnf install -y wget git unzip jq awscli

cd /opt

echo "Downloading Kafka..."
wget -O kafka_2.13-3.7.0.tgz https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz

tar -xzf kafka_2.13-3.7.0.tgz

mv kafka_2.13-3.7.0 kafka

mkdir -p /opt/kafka/libs

echo "Downloading MSK IAM Auth library..."

wget -O /opt/kafka/libs/aws-msk-iam-auth.jar \
https://github.com/aws/aws-msk-iam-auth/releases/download/v2.2.0/aws-msk-iam-auth-2.2.0-all.jar

cat <<EOF >/opt/kafka/client.properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler

EOF

cat <<'EOF' >/home/ec2-user/produce.sh
#!/bin/bash

BOOTSTRAP="$1"

TOPIC="${2:-demo-topic}"

MESSAGE="${3:-Hello From Producer}"

cd /opt/kafka

bin/kafka-topics.sh \
--bootstrap-server ${BOOTSTRAP} \
--command-config client.properties \
--create \
--if-not-exists \
--topic ${TOPIC}

echo "${MESSAGE}" | \
bin/kafka-console-producer.sh \
--bootstrap-server ${BOOTSTRAP} \
--producer.config client.properties \
--topic ${TOPIC}

echo "Message sent."
EOF

chmod +x /home/ec2-user/produce.sh
chown ec2-user:ec2-user /home/ec2-user/produce.sh
echo "Producer installation completed."