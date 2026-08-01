output "bootstrap_script_s3_uri" {
  description = "S3 URI of the uploaded bootstrap script"
  value       = "s3://${var.scripts_bucket}/bootstrap/bootstrap.sh"
}

output "streaming_job_s3_uri" {
  description = "S3 URI of the uploaded streaming job"
  value       = "s3://${var.scripts_bucket}/etl/streaming_job.py"
}

output "config_s3_uri" {
  description = "S3 URI of uploaded config.py"
  value       = "s3://${var.scripts_bucket}/etl/config.py"
}

output "schema_s3_uri" {
  description = "S3 URI of uploaded schema.py"
  value       = "s3://${var.scripts_bucket}/etl/schema.py"
}

output "utils_s3_uri" {
  description = "S3 URI of uploaded utils.py"
  value       = "s3://${var.scripts_bucket}/etl/utils.py"
}

output "topic_name" {
  description = "Kafka topic consumed by the streaming job"
  value       = var.topic_name
}

output "bootstrap_servers" {
  description = "MSK bootstrap brokers"
  value       = var.bootstrap_servers
}

output "emr_cluster_id" {
  description = "EMR Cluster ID"
  value       = var.emr_cluster_id
}

output "spark_job_submission_trigger" {
  description = "Terraform trigger indicating EMR job submission was executed"
  value       = null_resource.submit_job.id
}