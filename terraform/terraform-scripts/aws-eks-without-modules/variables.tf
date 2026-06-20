variable "aws_region" {
  default = "ap-south-1"
}

variable "cluster_name" {
  type = string
}

variable "cluster_version" {
  default = "1.36"
}

variable "vpc_cidr" {
  default = "10.0.0.0/16"
}

variable "public_subnets" {
  type = list(string)
}

variable "private_subnets" {
  type = list(string)
}

variable "instance_types" {
  default = ["t2.medium"]
}

variable "desired_size" {
  default = 4
}

variable "min_size" {
  default = 1
}

variable "max_size" {
  default = 4
}