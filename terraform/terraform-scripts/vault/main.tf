data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

module "vault" {
  source = "./modules/vault"

  instance_type = var.instance_type
  key_name      = var.key_name
  subnet_id     = data.aws_subnets.default.ids[0]

  vault_port = var.vault_port
  ssh_port   = var.ssh_port

  allowed_cidr = var.allowed_cidr
}