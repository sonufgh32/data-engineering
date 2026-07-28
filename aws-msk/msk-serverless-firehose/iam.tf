############################################
# Firehose Assume Role
############################################

data "aws_iam_policy_document" "firehose_assume" {
  # Trust policy that allows only the Firehose service to assume this role.
  statement {

    effect = "Allow"

    principals {

      type = "Service"

      identifiers = [

        "firehose.amazonaws.com"

      ]

    }

    actions = [

      "sts:AssumeRole"

    ]

  }

}

############################################
# Firehose Role
############################################

resource "aws_iam_role" "firehose" {
  # Execution identity used for reading MSK and delivering records to S3.
  name = "${local.common_name}-firehose-role"

  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json

}

############################################
# Firehose Policy
############################################

data "aws_iam_policy_document" "firehose" {
  # Least-privilege permissions for the Firehose delivery lifecycle.
  statement {

    sid = "S3Access"

    effect = "Allow"

    actions = [

      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject"

    ]

    resources = [

      aws_s3_bucket.firehose.arn,
      "${aws_s3_bucket.firehose.arn}/*"

    ]
  }

  statement {

    sid = "CloudWatch"

    effect = "Allow"

    actions = [

      "logs:PutLogEvents",
      "logs:CreateLogStream"

    ]

    resources = ["*"]

  }

  statement {
    sid = "MSK"

    effect = "Allow"

    actions = [
      "kafka:GetBootstrapBrokers",
      "kafka:DescribeCluster",
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster"
    ]

    resources = [
      aws_msk_serverless_cluster.this.arn
    ]
  }

  statement {
    sid = "ReadFirehoseSourceTopic"

    effect = "Allow"

    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData"
    ]

    resources = [
      "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/")}/${var.topic_name}"
    ]
  }

  statement {
    sid = "DescribeFirehoseConsumerGroups"

    effect = "Allow"

    actions = [
      "kafka-cluster:DescribeGroup"
    ]

    resources = [
      "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")}/*"
    ]
  }

}

resource "aws_iam_policy" "firehose" {
  # Create a customer-managed policy so its permissions are explicit and reviewable.
  name = "${local.common_name}-firehose"

  policy = data.aws_iam_policy_document.firehose.json

}

resource "aws_iam_role_policy_attachment" "firehose" {
  # Attach the delivery policy to the role assumed by Firehose.
  role = aws_iam_role.firehose.name

  policy_arn = aws_iam_policy.firehose.arn

}

