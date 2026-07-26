###############################################
# Availability Zones
###############################################

data "aws_availability_zones" "available" {
  state = "available"
}

###############################################
# VPC
###############################################

resource "aws_vpc" "kafka" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

###############################################
# Internet Gateway
###############################################

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.kafka.id
  tags = {
    Name = "${var.project_name}-igw"
  }
}

###############################################
# Elastic IP for NAT
###############################################

resource "aws_eip" "nat" {
  domain = "vpc"
  tags = {
    Name = "${var.project_name}-nat-eip"
  }

  depends_on = [
    aws_internet_gateway.igw
  ]
}

###############################################
# Public Subnets
###############################################

resource "aws_subnet" "public" {
  count                   = length(var.public_subnets)
  vpc_id                  = aws_vpc.kafka.id
  cidr_block              = var.public_subnets[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-${count.index + 1}"
    Tier = "Public"
  }
}

###############################################
# Private Subnets
###############################################

resource "aws_subnet" "private" {
  count                   = length(var.private_subnets)
  vpc_id                  = aws_vpc.kafka.id
  cidr_block              = var.private_subnets[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.project_name}-private-${count.index + 1}"
    Tier = "Private"
  }
}

###############################################
# NAT Gateway
###############################################

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags = {
    Name = "${var.project_name}-nat"
  }

  depends_on = [
    aws_internet_gateway.igw
  ]
}

###############################################
# Public Route Table
###############################################

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.kafka.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

###############################################
# Private Route Table
###############################################

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.kafka.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }

  tags = {
    Name = "${var.project_name}-private-rt"
  }
}

###############################################
# Public Route Associations
###############################################

resource "aws_route_table_association" "public" {
  count          = length(var.public_subnets)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

###############################################
# Private Route Associations
###############################################

resource "aws_route_table_association" "private" {
  count          = length(var.private_subnets)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

###############################################
# Get Latest Amazon Linux 2023 AMI
###############################################

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

###############################################
# Generate SSH Key Pair
###############################################

resource "tls_private_key" "kafka" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "kafka" {
  key_name   = "${var.project_name}-key"
  public_key = tls_private_key.kafka.public_key_openssh
}

resource "local_file" "private_key" {
  filename        = "${path.module}/${var.project_name}.pem"
  content         = tls_private_key.kafka.private_key_pem
  file_permission = "0400"
}

###############################################
# Producer Security Group
###############################################

resource "aws_security_group" "producer" {
  name        = "${var.project_name}-producer-sg"
  description = "Producer Security Group"
  vpc_id      = aws_vpc.kafka.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_ingress_ip]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-producer-sg"
  }
}

###############################################
# Consumer Security Group
###############################################

resource "aws_security_group" "consumer" {
  name        = "${var.project_name}-consumer-sg"
  description = "Consumer Security Group"
  vpc_id      = aws_vpc.kafka.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_ingress_ip]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-consumer-sg"
  }
}

###############################################
# MSK Security Group
###############################################

resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg"
  description = "MSK Security Group"
  vpc_id      = aws_vpc.kafka.id
  ingress {
    from_port = 9098
    to_port   = 9098
    protocol  = "tcp"
    security_groups = [
      aws_security_group.producer.id,
      aws_security_group.consumer.id
    ]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-msk-sg"
  }
}

###############################################
# Assume Role Policy
###############################################

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      type = "Service"
      identifiers = [
        "ec2.amazonaws.com"
      ]
    }
  }
}

###############################################
# Producer IAM Role
###############################################

resource "aws_iam_role" "producer" {
  name               = "${var.project_name}-producer-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

###############################################
# Consumer IAM Role
###############################################

resource "aws_iam_role" "consumer" {
  name               = "${var.project_name}-consumer-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

###############################################
# SSM Policy
###############################################

resource "aws_iam_role_policy_attachment" "producer_ssm" {
  role       = aws_iam_role.producer.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "consumer_ssm" {
  role       = aws_iam_role.consumer.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

###############################################
# MSK IAM Policy
###############################################

resource "aws_iam_policy" "msk_access" {
  name = "${var.project_name}-msk-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka:GetBootstrapBrokers",
          "kafka:DescribeCluster",
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",
          "kafka-cluster:CreateTopic"
        ]
        Resource = "*"
      }
    ]
  })
}

###############################################
# Attach MSK Policy
###############################################

resource "aws_iam_role_policy_attachment" "producer_msk" {
  role       = aws_iam_role.producer.name
  policy_arn = aws_iam_policy.msk_access.arn
}

resource "aws_iam_role_policy_attachment" "consumer_msk" {
  role       = aws_iam_role.consumer.name
  policy_arn = aws_iam_policy.msk_access.arn
}

###############################################
# Instance Profiles
###############################################

resource "aws_iam_instance_profile" "producer" {
  name = "${var.project_name}-producer-profile"
  role = aws_iam_role.producer.name
}

resource "aws_iam_instance_profile" "consumer" {
  name = "${var.project_name}-consumer-profile"
  role = aws_iam_role.consumer.name
}

###############################################
# KMS Key
###############################################

resource "aws_kms_key" "msk" {
  description             = "KMS Key for MSK"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "${var.project_name}-kms"
  }
}

###############################################
# CloudWatch Log Group
###############################################

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project_name}"
  retention_in_days = 14
}

###############################################
# MSK Configuration
###############################################

resource "aws_msk_configuration" "config" {
  kafka_versions = [
    var.kafka_version
  ]

  name              = "${var.project_name}-configuration"
  server_properties = <<PROPERTIES
auto.create.topics.enable=true
delete.topic.enable=true
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
log.retention.hours=168
PROPERTIES
}

###############################################
# MSK Cluster
###############################################

resource "aws_msk_cluster" "cluster" {
  cluster_name           = "${var.project_name}-cluster"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type = var.broker_instance_type
    client_subnets = [
      aws_subnet.private[0].id,
      aws_subnet.private[1].id,
      aws_subnet.private[2].id
    ]

    security_groups = [
      aws_security_group.msk.id
    ]

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.config.arn
    revision = aws_msk_configuration.config.latest_revision
  }

  tags = {
    Name = "${var.project_name}-cluster"
  }
}

###############################################
# Producer EC2
###############################################

resource "aws_instance" "producer" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type
  subnet_id     = aws_subnet.public[0].id
  key_name      = aws_key_pair.kafka.key_name
  vpc_security_group_ids = [
    aws_security_group.producer.id
  ]
  iam_instance_profile        = aws_iam_instance_profile.producer.name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/producer.sh")
  tags = {
    Name = "producer"
  }

  depends_on = [
    aws_msk_cluster.cluster
  ]
}

###############################################
# Consumer EC2
###############################################

resource "aws_instance" "consumer" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type
  subnet_id     = aws_subnet.public[1].id
  key_name      = aws_key_pair.kafka.key_name
  vpc_security_group_ids = [
    aws_security_group.consumer.id
  ]

  iam_instance_profile        = aws_iam_instance_profile.consumer.name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/consumer.sh")

  tags = {
    Name = "consumer"
  }

  depends_on = [
    aws_msk_cluster.cluster
  ]
}