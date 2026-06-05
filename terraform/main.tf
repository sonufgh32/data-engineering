terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

# Retrieves information about the AWS identity Terraform is using.
# Key value used here: data.aws_caller_identity.current.account_id
# That gives the current AWS account ID dynamically.
data "aws_caller_identity" "current" {}

# Retrieves the AWS partition for the current provider region.
# Common values:
#   aws for standard AWS
#   aws-cn for China
#   aws-us-gov for GovCloud
# Useful when building ARNs that must work across partitions.
data "aws_partition" "current" {}

# Defines a reusable local value.
# Creates a unique bucket name by embedding the current AWS account ID.
# This avoids hard-coding a name and reduces bucket name collisions.
locals {
  bucket_name = "my-demo-bucket-${data.aws_caller_identity.current.account_id}"
}

resource "aws_iam_role" "data_engineer_role" {
  name = "DataEngineerRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "DataEngineerRole"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket" "bucket" {
  bucket = local.bucket_name

  tags = {
    Name        = local.bucket_name
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  bucket = aws_s3_bucket.bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "public_access" {
  bucket = aws_s3_bucket.bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "bucket_policy" {
  bucket = aws_s3_bucket.bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowRoleReadAccess"
        Effect = "Allow"

        Principal = {
          AWS = aws_iam_role.data_engineer_role.arn
        }

        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]

        Resource = [
          aws_s3_bucket.bucket.arn,
          "${aws_s3_bucket.bucket.arn}/*"
        ]
      }
    ]
  })
}

output "bucket_name" {
  value = aws_s3_bucket.bucket.bucket
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "region" {
  value = aws_s3_bucket.bucket.region
}

output "role_arn" {
  value = aws_iam_role.data_engineer_role.arn
}

output "bucket_arn" {
  value = aws_s3_bucket.bucket.arn
}

output "partition" {
  value = data.aws_partition.current.partition
}