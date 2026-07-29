############################################
# CloudWatch Log Group
############################################

resource "aws_cloudwatch_log_group" "firehose" {
  # Firehose delivery errors and status messages are retained for seven days.
  name = local.cloudwatch_log_group

  retention_in_days = 7

}

resource "aws_cloudwatch_log_stream" "firehose" {
  # Keep this stream stable so delivery diagnostics have one predictable location.
  name = "S3Delivery"

  log_group_name = aws_cloudwatch_log_group.firehose.name

}

############################################
# Firehose Delivery Stream
############################################

resource "aws_kinesis_firehose_delivery_stream" "msk" {
  # Consume records from MSK, transform JSON to CSV, and deliver to S3.
  name = "${local.common_name}-firehose"

  destination = "extended_s3"

  ##########################################
  # Amazon MSK Source
  ##########################################

  # Firehose creates a private VPC connection and authenticates with its IAM role.
  msk_source_configuration {

    msk_cluster_arn = aws_msk_serverless_cluster.this.arn

    topic_name = var.topic_name

    authentication_configuration {

      role_arn = aws_iam_role.firehose.arn

      connectivity = "PRIVATE"

    }

  }

  ##########################################
  # S3 Destination
  ##########################################

  # Transform records before writing uncompressed CSV files to S3.
  extended_s3_configuration {

    role_arn = aws_iam_role.firehose.arn

    bucket_arn = aws_s3_bucket.firehose.arn

    buffering_size = var.buffer_size

    buffering_interval = var.buffer_interval

    compression_format = "UNCOMPRESSED"

    file_extension = ".csv"

    prefix = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    error_output_prefix = "errors/"

    processing_configuration {

      enabled = true

      processors {

        type = "Lambda"

        parameters {

          parameter_name  = "LambdaArn"
          parameter_value = aws_lambda_function.firehose_csv_transformer.arn

        }

      }

    }

    cloudwatch_logging_options {

      enabled = true

      log_group_name = aws_cloudwatch_log_group.firehose.name

      log_stream_name = aws_cloudwatch_log_stream.firehose.name

    }

  }

}