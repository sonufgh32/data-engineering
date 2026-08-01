resource "aws_security_group" "msk" {
  name                   = "msk-security-group"
  description            = "Security Group for MSK"
  vpc_id                 = var.vpc_id
  revoke_rules_on_delete = true

  tags = {
    Name = "msk-security-group"
  }
}

resource "aws_security_group" "emr_master" {

  name                   = "emr-master-security-group"
  description            = "EMR Master"
  vpc_id                 = var.vpc_id
  revoke_rules_on_delete = true

  tags = {
    Name                                       = "emr-master-security-group"
    "for-use-with-amazon-emr-managed-policies" = "true"
  }

}

resource "aws_security_group" "emr_core" {

  name                   = "emr-core-security-group"
  description            = "EMR Core"
  vpc_id                 = var.vpc_id
  revoke_rules_on_delete = true

  tags = {
    Name                                       = "emr-core-security-group"
    "for-use-with-amazon-emr-managed-policies" = "true"
  }

}

resource "aws_vpc_security_group_ingress_rule" "kafka_tls" {

  security_group_id = aws_security_group.msk.id

  referenced_security_group_id = aws_security_group.emr_master.id

  from_port = 9098

  to_port = 9098

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_ingress_rule" "kafka_tls_core" {

  security_group_id = aws_security_group.msk.id

  referenced_security_group_id = aws_security_group.emr_core.id

  from_port = 9098

  to_port = 9098

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_ingress_rule" "master_to_core" {

  security_group_id = aws_security_group.emr_core.id

  referenced_security_group_id = aws_security_group.emr_master.id

  from_port = 0

  to_port = 65535

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_ingress_rule" "core_to_master" {

  security_group_id = aws_security_group.emr_master.id

  referenced_security_group_id = aws_security_group.emr_core.id

  from_port = 0

  to_port = 65535

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_ingress_rule" "master_self" {

  security_group_id = aws_security_group.emr_master.id

  referenced_security_group_id = aws_security_group.emr_master.id

  from_port = 0

  to_port = 65535

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_ingress_rule" "core_self" {

  security_group_id = aws_security_group.emr_core.id

  referenced_security_group_id = aws_security_group.emr_core.id

  from_port = 0

  to_port = 65535

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_ingress_rule" "ssh" {

  security_group_id = aws_security_group.emr_master.id

  cidr_ipv4 = "0.0.0.0/0"

  from_port = 22

  to_port = 22

  ip_protocol = "tcp"

}

resource "aws_vpc_security_group_egress_rule" "msk_outbound" {

  security_group_id = aws_security_group.msk.id

  cidr_ipv4 = "0.0.0.0/0"

  ip_protocol = "-1"

}

resource "aws_vpc_security_group_egress_rule" "master_outbound" {

  security_group_id = aws_security_group.emr_master.id

  cidr_ipv4 = "0.0.0.0/0"

  ip_protocol = "-1"

}

resource "aws_vpc_security_group_egress_rule" "core_outbound" {

  security_group_id = aws_security_group.emr_core.id

  cidr_ipv4 = "0.0.0.0/0"

  ip_protocol = "-1"

}

# EMR requires this group when a cluster in a private subnet uses custom groups.
resource "aws_security_group" "emr_service_access" {
  name                   = "emr-service-access-security-group"
  description            = "EMR service access"
  vpc_id                 = var.vpc_id
  revoke_rules_on_delete = true

  tags = {
    Name                                       = "emr-service-access-security-group"
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
}

resource "aws_vpc_security_group_ingress_rule" "service_access_from_master" {
  security_group_id            = aws_security_group.emr_service_access.id
  referenced_security_group_id = aws_security_group.emr_master.id
  from_port                    = 9443
  to_port                      = 9443
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "service_access_outbound" {
  security_group_id = aws_security_group.emr_service_access.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}