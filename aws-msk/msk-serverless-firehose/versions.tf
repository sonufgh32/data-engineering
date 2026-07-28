# Pin Terraform and provider compatibility so plans are reproducible across machines.
terraform {
  required_version = ">= 1.6"

  required_providers {

    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }
}