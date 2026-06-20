variable "aws_region" {
  default = "ap-south-1"
}

variable "instance_type" {
  type = map(string)

  default = {
    dev   = "t3.micro"
    qa    = "t3.small"
    stage = "t3.medium"
    prod  = "t3.large"
  }
}

variable "instance_count" {
  type = map(number)

  default = {
    dev   = 1
    qa    = 1
    stage = 2
    prod  = 3
  }
}