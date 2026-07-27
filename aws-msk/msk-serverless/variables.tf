variable "aws_region" {
  default = "ap-south-1"
}

variable "project_name" {
  default = "msk-serverless-demo"
}

variable "firehose_name" {
  default = "msk-to-s3-firehose"
}

variable "kafka_topic" {
  default = "Testing"
}

variable "bucket_name" {
  default = "shivchoudhury-datasets"
}

variable "environment" {
  default = "dev"
}

variable "vpc_cidr" {
  default = "10.0.0.0/16"
}

variable "public_subnets" {
  type = list(string)
  default = [
    "10.0.1.0/24",
    "10.0.2.0/24"
  ]
}

variable "private_subnets" {
  type = list(string)
  default = [
    "10.0.10.0/24",
    "10.0.11.0/24"
  ]
}

variable "instance_type" {
  default = "t3.medium"
}

variable "ssh_ingress_ip" {
  default = "0.0.0.0/0"
}