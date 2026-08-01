resource "random_string" "suffix" {

  length = 6

  upper = false

  special = false

  numeric = true

}

resource "aws_s3_bucket" "scripts" {

  bucket = "${var.project_name}-scripts-${random_string.suffix.result}"

  force_destroy = true

}

resource "aws_s3_bucket" "output" {

  bucket = "${var.project_name}-output-${random_string.suffix.result}"

  force_destroy = true

}

resource "aws_s3_bucket" "logs" {

  bucket = "${var.project_name}-logs-${random_string.suffix.result}"

  force_destroy = true

}

resource "aws_s3_bucket_versioning" "scripts" {

  bucket = aws_s3_bucket.scripts.id

  versioning_configuration {

    status = "Enabled"

  }

}

resource "aws_s3_bucket_versioning" "output" {

  bucket = aws_s3_bucket.output.id

  versioning_configuration {

    status = "Enabled"

  }

}

resource "aws_s3_bucket_versioning" "logs" {

  bucket = aws_s3_bucket.logs.id

  versioning_configuration {

    status = "Enabled"

  }

}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {

  bucket = aws_s3_bucket.scripts.id

  rule {

    apply_server_side_encryption_by_default {

      sse_algorithm = "AES256"

    }

  }

}

resource "aws_s3_bucket_server_side_encryption_configuration" "output" {

  bucket = aws_s3_bucket.output.id

  rule {

    apply_server_side_encryption_by_default {

      sse_algorithm = "AES256"

    }

  }

}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {

  bucket = aws_s3_bucket.logs.id

  rule {

    apply_server_side_encryption_by_default {

      sse_algorithm = "AES256"

    }

  }

}

resource "aws_s3_bucket_public_access_block" "scripts" {

  bucket = aws_s3_bucket.scripts.id

  block_public_acls = true

  block_public_policy = true

  ignore_public_acls = true

  restrict_public_buckets = true

}

resource "aws_s3_bucket_public_access_block" "output" {

  bucket = aws_s3_bucket.output.id

  block_public_acls = true

  block_public_policy = true

  ignore_public_acls = true

  restrict_public_buckets = true

}

resource "aws_s3_bucket_public_access_block" "logs" {

  bucket = aws_s3_bucket.logs.id

  block_public_acls = true

  block_public_policy = true

  ignore_public_acls = true

  restrict_public_buckets = true

}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {

  bucket = aws_s3_bucket.logs.id

  rule {

    id = "delete-old-logs"

    status = "Enabled"

    filter {}

    expiration {

      days = 30

    }

  }

}