output "workspace" {
  value = terraform.workspace
}

output "instance_ids" {
  value = aws_instance.web[*].id
}

output "vpc_id" {
  value = aws_vpc.main.id
}

output "subnet_id" {
  value = aws_subnet.public.id
}

output "security_group_id" {
  value = aws_security_group.web.id
}

output "igw_id" {
  value = aws_internet_gateway.main.id
}

output "public_ip" {
  value = aws_instance.web[*].public_ip
}