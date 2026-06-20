variable "instance_type" {}
variable "key_name" {}
variable "subnet_id" {}

variable "vault_port" {}
variable "ssh_port" {}

variable "allowed_cidr" {
  type = list(string)
}