resource "aws_cloudwatch_log_group" "msk" {

  name              = "/aws/msk/${var.project_name}"
  retention_in_days = 30

}

resource "aws_msk_serverless_cluster" "this" {

  cluster_name = "${var.project_name}-serverless"

  vpc_config {

    subnet_ids = var.private_subnets

    security_group_ids = [
      var.security_group_id
    ]
  }

  client_authentication {

    sasl {

      iam {
        enabled = true
      }

    }

  }

  tags = {
    Environment = "dev"
    Project     = var.project_name
  }

}