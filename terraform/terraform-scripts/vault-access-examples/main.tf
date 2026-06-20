variable "aws_region" {
  default = "ap-south-1"
}

provider "aws" {
  region = var.aws_region
}

provider "vault" {
  address = "http://3.110.174.63:8200"

  skip_child_token = true

  auth_login {
    path = "auth/approle/login"
    parameters = {
      role_id   = "c2827e2f-d4cb-ea01-ad1c-2052ca8f9f92"
      secret_id = "24e469ea-edba-4b70-2078-b69ac430cd13"
    }
  }
}

data "vault_kv_secret_v2" "example" {
  mount = "kv"
  name  = "test-secret"
}

output "secret" {
  value     = data.vault_kv_secret_v2.example.data["username"]
  sensitive = true
}

output "secret2" {
  value     = nonsensitive(data.vault_kv_secret_v2.example.data)
  sensitive = false
}