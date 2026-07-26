output "producer_instance_id" {
  value = aws_instance.producer.id
}

output "producer_public_ip" {
  value = aws_instance.producer.public_ip
}

output "producer_ssh_command" {
  value = "ssh -i ./${local_file.private_key.filename} ec2-user@${aws_instance.producer.public_ip}"
}

output "consumer_instance_id" {
  value = aws_instance.consumer.id
}

output "consumer_public_ip" {
  value = aws_instance.consumer.public_ip
}

output "consumer_ssh_command" {
  value = "ssh -i ./${local_file.private_key.filename} ec2-user@${aws_instance.consumer.public_ip}"
}

output "bootstrap_brokers" {
  value = aws_msk_serverless_cluster.cluster.bootstrap_brokers_sasl_iam
}
