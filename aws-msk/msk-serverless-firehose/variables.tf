#############################
# General
#############################

variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Project Name"
  type        = string
  default     = "msk-firehose-demo"
}

variable "environment" {
  description = "Environment"
  type        = string
  default     = "dev"
}

variable "kafka_allowed_cidrs" {
  description = "CIDR blocks allowed to reach MSK on port 9098. Include VPC CIDRs and any VPN client CIDR ranges."
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

#############################
# Networking
#############################

# variable "vpc_id" {
#   description = "Existing VPC ID"
#   type        = string
# }

# variable "private_subnet_ids" {
#   description = "Private subnet IDs"
#   type        = list(string)
# }

#############################
# S3
#############################

variable "bucket_name" {
  description = "Destination bucket"
  type        = string
}

#############################
# Kafka
#############################

variable "topic_name" {
  description = "MSK topic consumed by Firehose and used by the sample producer"
  type        = string
  default     = "sample-topic"
}

#############################
# Firehose
#############################

variable "buffer_size" {
  description = "Firehose buffer size in MiBs before writing an S3 object"
  default     = 64
}

variable "buffer_interval" {
  description = "Maximum Firehose buffering period in seconds before an S3 write"
  default     = 60
}

#############################
# Producer EC2
#############################

variable "producer_instance_type" {
  description = "EC2 instance type for Kafka producer host"
  type        = string
  default     = "t3.micro"
}

variable "producer_key_name" {
  description = "Optional EC2 key pair name for SSH access"
  type        = string
  default     = ""
}

variable "producer_ssh_allowed_cidrs" {
  description = "CIDR blocks allowed to SSH to the producer instance"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "producer_public_subnet_cidr" {
  description = "CIDR block for public subnet hosting producer instance"
  type        = string
  default     = "10.0.10.0/24"
}

variable "producer_public_subnet_az" {
  description = "Availability zone for producer public subnet"
  type        = string
  default     = "ap-south-1a"
}

#############################
# Kafka UI EC2
#############################

variable "kafka_ui_instance_type" {
  description = "EC2 instance type for the public Kafka UI host"
  type        = string
  default     = "t3.small"
}

variable "kafka_ui_image" {
  description = "Kafka UI container image"
  type        = string
  default     = "provectuslabs/kafka-ui:v0.7.2"
}

variable "kafka_ui_allowed_cidrs" {
  description = "CIDR blocks allowed to access Kafka UI on port 8080; restrict this to trusted public IP ranges."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}