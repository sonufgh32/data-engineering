variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "project_name" {
  type    = string
  default = "kafka-project"
}

variable "environment" {
  type    = string
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
    "10.0.11.0/24",
    "10.0.12.0/24"
  ]
}

variable "instance_type" {
  default = "t3.small"
}

variable "broker_instance_type" {
  default = "kafka.t3.small"
}

variable "broker_count" {
  default = 2
}

variable "ebs_volume_size" {
  default = 100
}

variable "kafka_version" {
  default = "3.7.x"
}

variable "ssh_ingress_ip" {
  description = "Your public IP/CIDR for SSH"
  default     = "0.0.0.0/0"
}