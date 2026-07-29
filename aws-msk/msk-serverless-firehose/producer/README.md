# MSK Serverless Producer

Python utilities for administering a topic and publishing sample JSON records to an Amazon MSK Serverless cluster using IAM authentication.

Run these scripts from the Terraform-created producer EC2 instance or another compute environment with network access to the MSK VPC. MSK Serverless bootstrap brokers are private; the scripts will not connect from a typical local workstation without VPN or Direct Connect access to that VPC.

## Prerequisites

1. Deploy the parent Terraform stack.
2. Have AWS CLI credentials that can start an SSM session, or configure `producer_key_name` and a restricted `producer_ssh_allowed_cidrs` value for SSH.
3. For manual execution, use Python 3 and install [requirements.txt](requirements.txt).

Get the values created by Terraform:

```bash
terraform output -raw producer_ssm_start_session_command
terraform output -raw bootstrap_brokers_sasl_iam
terraform output -raw bucket_name
```

Open the recommended SSM shell with the first command's output. The instance bootstrap places this directory at `/opt/msk-producer`, creates a virtual environment, and installs a convenient `msk-producer-run` command.

```bash
aws ssm start-session --target i-0123456789abcdef0 --region ap-south-1
cd /opt/msk-producer
```

## Script Reference

| File | Purpose | Run directly? |
| --- | --- | --- |
| `config.py` | Region, topic, endpoint, partition, replication, and interval defaults. Environment values override its defaults. | No; imported by the CLI scripts. |
| `kafka_utils.py` | IAM token provider and reusable topic/producer operations. | No; imported by the CLI scripts. |
| `create_topic.py` | Creates, lists, or deletes MSK topics. | Yes. |
| `producer.py` | Sends generated sensor JSON records. | Yes. |
| `run_on_ec2.sh` | Installs dependencies in `.venv` and starts the producer. | Yes. |

CLI values take precedence over the defaults in `config.py`. On the Terraform-created producer, source `/etc/profile.d/msk_producer_env.sh` to use the current cluster endpoint, topic, Region, and interval automatically.

## `create_topic.py`

Use this script to administer topics. The producer EC2 role includes the required MSK IAM permissions.

```bash
# Create the Firehose source topic with its default one partition and replication factor of three.
source .venv/bin/activate
python create_topic.py create sample-topic \
	--bootstrap "$(terraform output -raw bootstrap_brokers_sasl_iam)"

# List topics visible to the producer role.
python create_topic.py list \
	--bootstrap "$(terraform output -raw bootstrap_brokers_sasl_iam)"

# Create a topic with an explicit partition count.
python create_topic.py create orders \
	--partitions 3 --replication 3 \
	--bootstrap "$(terraform output -raw bootstrap_brokers_sasl_iam)"

# Delete a non-production topic.
python create_topic.py delete orders \
	--bootstrap "$(terraform output -raw bootstrap_brokers_sasl_iam)"
```

When connected through SSM, Terraform is usually not installed on the instance. Use the endpoint already placed in `/etc/profile.d/msk_producer_env.sh` instead:

```bash
source /etc/profile.d/msk_producer_env.sh
source .venv/bin/activate
python create_topic.py create "$TOPIC" --bootstrap "$BOOTSTRAP_SERVERS"
```

## `producer.py`

The `run` command creates the topic when needed, then sends a JSON payload containing sensor measurements and a UTC timestamp. With no `--count`, it runs until interrupted with `Ctrl+C`.

```bash
source /etc/profile.d/msk_producer_env.sh
source .venv/bin/activate

# Send records continuously every five seconds.
python producer.py run --topic "$TOPIC" --interval 5 --bootstrap "$BOOTSTRAP_SERVERS"

# Send exactly ten records every two seconds, useful for a quick delivery test.
python producer.py run --topic "$TOPIC" --interval 2 --count 10 --bootstrap "$BOOTSTRAP_SERVERS"

# Print a generated payload without connecting to MSK.
python producer.py sample

# Skip the topic creation check when the topic has already been created.
python producer.py run --topic "$TOPIC" --count 10 --skip-create-topic --bootstrap "$BOOTSTRAP_SERVERS"
```

## `run_on_ec2.sh`

Run this when you copied the producer directory to a supported Amazon Linux EC2 instance and want the script to install its own virtual environment. It supports `dnf` and `yum` hosts.

```bash
chmod +x run_on_ec2.sh

# Arguments: bootstrap servers, optional topic, optional interval, optional count.
./run_on_ec2.sh \
	"boot-xxxx.c1.kafka-serverless.ap-south-1.amazonaws.com:9098" \
	sample-topic 2 10
```

Omit the final count to keep producing until you stop the process:

```bash
./run_on_ec2.sh "boot-xxxx.c1.kafka-serverless.ap-south-1.amazonaws.com:9098" sample-topic 5
```

On the Terraform-created instance, dependencies are already installed. Use the preconfigured runner for the continuous default workflow:

```bash
msk-producer-run
```

## Verify Firehose Delivery

Firehose buffers records for up to the configured `buffer_interval` (60 seconds by default) or until the buffer size is reached. After sending records, wait for the interval and inspect the destination bucket:

```bash
aws s3 ls "s3://$(terraform output -raw bucket_name)/" --recursive
```

Successful objects are uncompressed CSV files organized as `year=YYYY/month=MM/day=DD/`. To investigate missing deliveries, inspect the Firehose log group and stream created by Terraform:

```bash
aws logs tail "/aws/kinesisfirehose/msk-firehose-demo-dev" --follow --region ap-south-1
```

Replace the log-group name and Region if you changed `project_name`, `environment`, or `aws_region`.
