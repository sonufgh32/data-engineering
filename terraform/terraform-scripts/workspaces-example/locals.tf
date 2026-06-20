locals {

  env_config = {
    dev = {
      cidr = "10.0.0.0/24"
    }

    qa = {
      cidr = "10.0.1.0/24"
    }

    stage = {
      cidr = "10.0.2.0/24"
    }

    prod = {
      cidr = "10.0.3.0/24"
    }
  }

  current_env = local.env_config[terraform.workspace]
}