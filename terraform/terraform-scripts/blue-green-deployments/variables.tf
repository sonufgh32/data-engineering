variable "aws_region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "public_subnets" {
  type = list(string)
}

variable "instance_type" {
  type = string
}

variable "ami_id" {
  type = string
}

variable "active_environment" {
  type = string

  validation {
    condition     = contains(["blue", "green"], var.active_environment)
    error_message = "Must be blue or green."
  }
}