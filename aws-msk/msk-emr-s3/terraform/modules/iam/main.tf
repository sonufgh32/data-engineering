resource "aws_iam_role" "emr_service_role" {

  name = "EMR-Service-Role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_policy" {

  role = aws_iam_role.emr_service_role.name

  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"

}


resource "aws_iam_role" "emr_ec2_role" {

  name = "EMR-EC2-Role"

  assume_role_policy = jsonencode({

    Version = "2012-10-17"

    Statement = [

      {

        Effect = "Allow"

        Principal = {

          Service = "ec2.amazonaws.com"

        }

        Action = "sts:AssumeRole"

      }

    ]

  })

}

resource "aws_iam_role_policy_attachment" "ec2_policy" {

  role = aws_iam_role.emr_ec2_role.name

  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"

}

# AmazonEMRServicePolicy_v2 only covers the default EC2 role name. Allow this
# service role to pass the project's explicitly named EMR EC2 role instead.
resource "aws_iam_role_policy" "emr_service_pass_ec2_role" {

  name = "EMR-Pass-EC2-Role"
  role = aws_iam_role.emr_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = "iam:PassRole"

        Resource = aws_iam_role.emr_ec2_role.arn

        Condition = {
          StringLike = {
            "iam:PassedToService" = "ec2.amazonaws.com*"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "s3_policy" {

  name = "EMR-S3-Policy"

  policy = jsonencode({

    Version = "2012-10-17"

    Statement = [

      {

        Effect = "Allow"

        Action = [

          "s3:*"

        ]

        Resource = "*"

      }

    ]

  })

}

resource "aws_iam_role_policy_attachment" "s3_attachment" {

  role = aws_iam_role.emr_ec2_role.name

  policy_arn = aws_iam_policy.s3_policy.arn

}


resource "aws_iam_policy" "cloudwatch" {

  name = "EMR-CloudWatch"

  policy = jsonencode({

    Version = "2012-10-17"

    Statement = [

      {

        Effect = "Allow"

        Action = [

          "logs:*"

        ]

        Resource = "*"

      }

    ]

  })

}

resource "aws_iam_role_policy_attachment" "cloudwatch_attachment" {

  role = aws_iam_role.emr_ec2_role.name

  policy_arn = aws_iam_policy.cloudwatch.arn

}

resource "aws_iam_policy" "glue" {

  name = "EMR-Glue"

  policy = jsonencode({

    Version = "2012-10-17"

    Statement = [

      {

        Effect = "Allow"

        Action = [

          "glue:*"

        ]

        Resource = "*"

      }

    ]

  })

}

resource "aws_iam_role_policy_attachment" "glue_attachment" {

  role = aws_iam_role.emr_ec2_role.name

  policy_arn = aws_iam_policy.glue.arn

}

resource "aws_iam_policy" "msk" {

  name = "EMR-MSK"

  policy = jsonencode({

    Version = "2012-10-17"

    Statement = [

      {

        Effect = "Allow"

        Action = [

          "kafka-cluster:*"

        ]

        Resource = "*"

      }

    ]

  })

}

resource "aws_iam_role_policy_attachment" "msk_attachment" {

  role = aws_iam_role.emr_ec2_role.name

  policy_arn = aws_iam_policy.msk.arn

}

resource "aws_iam_instance_profile" "emr_profile" {

  name = "EMR-Instance-Profile"

  role = aws_iam_role.emr_ec2_role.name

}