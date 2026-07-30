############################################
# MSK
############################################

output "msk_cluster_arn" {
  description = "MSK Serverless Cluster ARN"
  value       = aws_msk_serverless_cluster.this.arn
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap brokers for IAM authentication"
  value       = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}

############################################
# Firehose
############################################

output "firehose_name" {
  description = "Name of the MSK-to-S3 Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.msk.name
}

############################################
# S3
############################################

output "bucket_name" {
  description = "S3 bucket receiving compressed Firehose records"
  value       = aws_s3_bucket.firehose.bucket
}

############################################
# IAM
############################################

output "firehose_role_arn" {
  description = "IAM role assumed by the Firehose delivery stream"
  value       = aws_iam_role.firehose.arn
}

############################################
# Security Group
############################################

output "security_group_id" {
  description = "Security group attached to the MSK Serverless cluster"
  value       = aws_security_group.msk.id
}

############################################
# Producer EC2
############################################

output "producer_instance_id" {
  description = "EC2 instance ID of the preconfigured producer host"
  value       = aws_instance.producer.id
}

output "producer_instance_public_ip" {
  description = "Public IP address of the producer host, when allocated"
  value       = aws_instance.producer.public_ip
}

output "producer_instance_private_ip" {
  description = "Private IP address of the producer host inside the VPC"
  value       = aws_instance.producer.private_ip
}

output "producer_ssm_start_session_command" {
  description = "AWS CLI command for opening an SSM shell on the producer host"
  value       = "aws ssm start-session --target ${aws_instance.producer.id} --region ${var.aws_region}"
}

output "producer_code_path_on_ec2" {
  description = "Directory containing the producer scripts on the EC2 host"
  value       = "/opt/msk-producer"
}

############################################
# Kafka UI EC2
############################################

output "kafka_ui_instance_id" {
  description = "EC2 instance ID of the Kafka UI host"
  value       = aws_instance.kafka_ui.id
}

output "kafka_ui_public_ip" {
  description = "Public IP address of the Kafka UI host"
  value       = aws_instance.kafka_ui.public_ip
}

output "kafka_ui_url" {
  description = "Public Kafka UI URL"
  value       = "http://${aws_instance.kafka_ui.public_dns}:8080"
}

output "kafka_ui_ssm_start_session_command" {
  description = "AWS CLI command for opening an SSM shell on the Kafka UI host"
  value       = "aws ssm start-session --target ${aws_instance.kafka_ui.id} --region ${var.aws_region}"
}