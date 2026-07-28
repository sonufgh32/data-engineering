aws_region = "ap-south-1"

project_name = "msk-firehose-demo"

environment = "dev"

bucket_name = "shivchoudhury-datasets-kafka"

topic_name = "sample-topic"

buffer_size = 64

buffer_interval = 60

producer_instance_type = "t3.micro"

# Set this if you want SSH access, otherwise leave empty and use SSM Session Manager.
producer_key_name = ""

# Restrict this to your public IP CIDR for SSH (example shown below).
# producer_ssh_allowed_cidrs = ["203.0.113.10/32"]
producer_ssh_allowed_cidrs = ["0.0.0.0/0"]