############################################
# Kafka UI Security Group
############################################

resource "aws_security_group" "kafka_ui" {
  name        = "${local.common_name}-kafka-ui-sg"
  description = "Security group for the public Kafka UI EC2 instance"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.common_name}-kafka-ui-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "kafka_ui_http" {
  for_each = toset(var.kafka_ui_allowed_cidrs)

  security_group_id = aws_security_group.kafka_ui.id
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  cidr_ipv4         = each.value
  description       = "Allow Kafka UI web access"
}

resource "aws_vpc_security_group_egress_rule" "kafka_ui_all" {
  security_group_id = aws_security_group.kafka_ui.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "Allow outbound traffic for Kafka UI"
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_kafka_ui" {
  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = aws_security_group.kafka_ui.id
  ip_protocol                  = "tcp"
  from_port                    = 9098
  to_port                      = 9098
  description                  = "Allow Kafka UI to MSK IAM listener"
}

############################################
# Kafka UI Instance Role
############################################

data "aws_iam_policy_document" "kafka_ui_assume" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "kafka_ui" {
  name               = "${local.common_name}-kafka-ui-role"
  assume_role_policy = data.aws_iam_policy_document.kafka_ui_assume.json
}

data "aws_iam_policy_document" "kafka_ui_msk" {
  statement {
    sid    = "MskBootstrap"
    effect = "Allow"

    actions = [
      "kafka:GetBootstrapBrokers",
      "kafka:DescribeCluster",
      "kafka:DescribeClusterV2"
    ]

    resources = [aws_msk_serverless_cluster.this.arn]
  }

  statement {
    sid    = "MskUiDataPlane"
    effect = "Allow"

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:ReadData"
    ]

    resources = [
      aws_msk_serverless_cluster.this.arn,
      "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/")}/*",
      "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")}/*"
    ]
  }
}

resource "aws_iam_policy" "kafka_ui_msk" {
  name   = "${local.common_name}-kafka-ui-msk"
  policy = data.aws_iam_policy_document.kafka_ui_msk.json
}

resource "aws_iam_role_policy_attachment" "kafka_ui_msk" {
  role       = aws_iam_role.kafka_ui.name
  policy_arn = aws_iam_policy.kafka_ui_msk.arn
}

resource "aws_iam_role_policy_attachment" "kafka_ui_ssm" {
  role       = aws_iam_role.kafka_ui.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "kafka_ui" {
  name = "${local.common_name}-kafka-ui-profile"
  role = aws_iam_role.kafka_ui.name
}

############################################
# Kafka UI EC2
############################################

resource "aws_instance" "kafka_ui" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.kafka_ui_instance_type
  subnet_id                   = aws_subnet.producer_public.id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.kafka_ui.id]
  iam_instance_profile        = aws_iam_instance_profile.kafka_ui.name

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/templates/kafka_ui_user_data.sh.tftpl", {
    bootstrap_servers = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
    kafka_ui_image    = var.kafka_ui_image
  })

  depends_on = [
    aws_msk_serverless_cluster.this,
    aws_iam_role_policy_attachment.kafka_ui_msk,
    aws_iam_role_policy_attachment.kafka_ui_ssm
  ]

  tags = {
    Name = "${local.common_name}-kafka-ui"
  }
}