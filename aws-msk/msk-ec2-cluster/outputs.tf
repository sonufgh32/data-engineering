#########################################
# VPC
#########################################

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.kafka.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

#########################################
# Internet Gateway
#########################################

output "internet_gateway_id" {
  value = aws_internet_gateway.igw.id
}

#########################################
# NAT Gateway
#########################################

output "nat_gateway_id" {
  value = aws_nat_gateway.nat.id
}

output "nat_gateway_public_ip" {
  value = aws_eip.nat.public_ip
}

#########################################
# Security Groups
#########################################

output "producer_sg" {
  value = aws_security_group.producer.id
}

output "consumer_sg" {
  value = aws_security_group.consumer.id
}

output "msk_sg" {
  value = aws_security_group.msk.id
}

#########################################
# EC2
#########################################

output "producer_instance_id" {
  value = aws_instance.producer.id
}

output "producer_private_ip" {
  value = aws_instance.producer.private_ip
}

output "producer_public_ip" {
  value = aws_instance.producer.public_ip
}

output "consumer_instance_id" {
  value = aws_instance.consumer.id
}

output "consumer_private_ip" {
  value = aws_instance.consumer.private_ip
}

output "consumer_public_ip" {
  value = aws_instance.consumer.public_ip
}

#########################################
# IAM
#########################################

output "producer_role_arn" {
  value = aws_iam_role.producer.arn
}

output "consumer_role_arn" {
  value = aws_iam_role.consumer.arn
}

#########################################
# KMS
#########################################

output "kms_key_id" {
  value = aws_kms_key.msk.key_id
}

output "kms_key_arn" {
  value = aws_kms_key.msk.arn
}

#########################################
# MSK
#########################################

output "msk_cluster_name" {
  value = aws_msk_cluster.cluster.cluster_name
}

output "msk_cluster_arn" {
  value = aws_msk_cluster.cluster.arn
}

output "msk_current_version" {
  value = aws_msk_cluster.cluster.current_version
}

output "bootstrap_brokers_tls" {
  value = aws_msk_cluster.cluster.bootstrap_brokers_tls
}

output "bootstrap_brokers_sasl_iam" {
  value = aws_msk_cluster.cluster.bootstrap_brokers_sasl_iam
}

output "bootstrap_brokers_sasl_scram" {
  value = aws_msk_cluster.cluster.bootstrap_brokers_sasl_scram
}

output "zookeeper_connect_string" {
  value = aws_msk_cluster.cluster.zookeeper_connect_string
}

#########################################
# SSH
#########################################

output "private_key_file" {
  value = local_file.private_key.filename
}

#########################################
# Helpful Commands
#########################################

output "producer_ssh_command" {
  value = "ssh -i ${local_file.private_key.filename} ec2-user@${aws_instance.producer.public_ip}"
}

output "consumer_ssh_command" {
  value = "ssh -i ${local_file.private_key.filename} ec2-user@${aws_instance.consumer.public_ip}"
}