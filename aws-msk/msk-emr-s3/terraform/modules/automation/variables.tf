variable "scripts_bucket" {}

variable "output_bucket" {}

variable "emr_cluster_id" {}

variable "bootstrap_servers" {}

variable "topic_name" {
  default = "transactions"
}