############################################
# CloudWatch Logs
############################################

resource "aws_cloudwatch_log_group" "msk" {
  # Retain MSK-related logs for a week to balance troubleshooting and cost.
  name = "/aws/msk/${local.common_name}"

  retention_in_days = 7

}

############################################
# MSK Serverless
############################################

resource "aws_msk_serverless_cluster" "this" {
  # MSK Serverless exposes only IAM SASL authentication to connected clients.
  cluster_name = local.common_name

  client_authentication {

    sasl {

      iam {
        enabled = true
      }

    }

  }

  # The cluster is reachable only through the two private VPC subnets.
  vpc_config {
    subnet_ids = [
      aws_subnet.private_a.id,
      aws_subnet.private_b.id
    ]

    security_group_ids = [
      aws_security_group.msk.id
    ]
  }

  tags = {

    Name = local.common_name

  }

}

##################################################
# MSK Cluster Policy
##################################################
resource "aws_msk_cluster_policy" "firehose" {
  # MSK Serverless cluster policies authorize Firehose's VPC connection only.
  # Topic consumption is authorized by the Firehose role's identity policy.
  cluster_arn = aws_msk_serverless_cluster.this.arn

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "AllowFirehoseVpcConnection"
        Effect = "Allow"

        Principal = {
          Service = "firehose.amazonaws.com"
        }

        Action = [
          "kafka:CreateVpcConnection",
          "kafka:GetBootstrapBrokers"
        ]

        Resource = aws_msk_serverless_cluster.this.arn
      }
    ]
  })
}