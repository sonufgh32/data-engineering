variable "aws_region" {
  default = "ap-south-1"
}

variable "instance_type" {
  default = "t3.medium"
}

variable "vault_port" {
  default = 8200
}

variable "ssh_port" {
  default = 22
}

variable "allowed_cidr" {
  type = list(string)

  default = [
    "0.0.0.0/0"
  ]
}

variable "key_name" {
  type = string
}