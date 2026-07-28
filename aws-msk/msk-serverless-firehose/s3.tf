############################################
# S3 Bucket
############################################

resource "aws_s3_bucket" "firehose" {
  # Destination for Firehose records. force_destroy is convenient for demo teardown.
  bucket = var.bucket_name

  force_destroy = true

  tags = {
    Name = var.bucket_name
  }
}

############################################
# Versioning
############################################

resource "aws_s3_bucket_versioning" "firehose" {
  # Preserve overwritten delivery objects for recovery and auditability.
  bucket = aws_s3_bucket.firehose.id

  versioning_configuration {

    status = "Enabled"

  }
}

############################################
# Encryption
############################################

resource "aws_s3_bucket_server_side_encryption_configuration" "firehose" {
  # Encrypt objects at rest with S3-managed AES-256 keys.
  bucket = aws_s3_bucket.firehose.id

  rule {

    apply_server_side_encryption_by_default {

      sse_algorithm = "AES256"

    }

  }

}

############################################
# Block Public Access
############################################

resource "aws_s3_bucket_public_access_block" "firehose" {
  # Ensure delivery data cannot be exposed by bucket or object ACLs/policies.
  bucket = aws_s3_bucket.firehose.id

  block_public_acls = true

  block_public_policy = true

  ignore_public_acls = true

  restrict_public_buckets = true
}

############################################
# Lifecycle Rule
############################################

resource "aws_s3_bucket_lifecycle_configuration" "firehose" {
  # Move older demo data to cheaper storage tiers.
  bucket = aws_s3_bucket.firehose.id

  rule {

    id = "cleanup"

    status = "Enabled"

    filter {}

    transition {

      days = 30

      storage_class = "STANDARD_IA"

    }

    transition {

      days = 90

      storage_class = "GLACIER"

    }

  }

}