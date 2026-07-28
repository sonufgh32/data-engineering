############################################
# Security Group for MSK Serverless
############################################

resource "aws_security_group" "msk" {

  name        = "${local.common_name}-msk-sg"
  description = "Security Group for MSK Serverless"
  vpc_id      = aws_vpc.main.id

  # This group is attached directly to the MSK Serverless cluster.
  tags = {
    Name = "${local.common_name}-msk-sg"
  }
}

############################################
# Allow Kafka Traffic Inside VPC
############################################

resource "aws_vpc_security_group_ingress_rule" "kafka" {

  # Permit IAM-authenticated Kafka clients from the configured VPC or VPN CIDRs.
  for_each = toset(var.kafka_allowed_cidrs)

  security_group_id = aws_security_group.msk.id

  ip_protocol = "tcp"

  from_port = 9098

  to_port = 9098

  cidr_ipv4 = each.value

  description = "Kafka IAM TLS from allowed CIDR"
}

############################################
# Allow HTTPS
############################################

resource "aws_vpc_security_group_egress_rule" "https" {

  # Allow TLS egress used by AWS service integrations.
  security_group_id = aws_security_group.msk.id

  ip_protocol = "tcp"

  from_port = 443

  to_port = 443

  cidr_ipv4 = "0.0.0.0/0"

  description = "HTTPS"
}

############################################
# Allow Kafka Outbound
############################################

resource "aws_vpc_security_group_egress_rule" "kafka" {

  # Retain Kafka return-path connectivity for the allowed client networks.
  for_each = toset(var.kafka_allowed_cidrs)

  security_group_id = aws_security_group.msk.id

  ip_protocol = "tcp"

  from_port = 9098

  to_port = 9098

  cidr_ipv4 = each.value

  description = "Kafka to allowed CIDR"
}