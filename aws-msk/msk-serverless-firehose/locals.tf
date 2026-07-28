locals {
  # Shared name used to keep resources for one deployment easy to identify.
  common_name = "${var.project_name}-${var.environment}"

  # Firehose writes delivery diagnostics to this CloudWatch Logs group.
  cloudwatch_log_group = "/aws/kinesisfirehose/${local.common_name}"
  # Reserved prefix for data written to the destination bucket.
  s3_prefix = "msk-data/"

}