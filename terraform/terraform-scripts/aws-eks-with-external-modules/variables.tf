variable "aws_region" {
  default = "ap-south-1"
}

variable "cluster_name" {
  default = "demo-eks-cluster"
}

variable "cluster_version" {
  default = "1.36"
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
    "10.0.11.0/24",
    "10.0.12.0/24"
  ]
}

variable "node_instance_types" {
  type = list(string)

  default = ["t2.micro"]
}

variable "desired_size" {
  default = 2
}

variable "min_size" {
  default = 1
}

variable "max_size" {
  default = 3
}