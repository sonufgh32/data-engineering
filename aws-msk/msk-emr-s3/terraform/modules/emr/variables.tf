variable "project_name" {
  type = string
}

variable "private_subnets" {
  type = list(string)
}

variable "master_security_group" {
  type = string
}

variable "core_security_group" {
  type = string
}

variable "service_access_security_group" {
  type = string
}

variable "service_role" {
  type = string
}

variable "instance_profile" {
  type = string
}

variable "logs_bucket" {
  type = string
}

variable "scripts_bucket" {
  type = string
}