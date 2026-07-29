############################################
# Producer AMI
############################################

data "aws_ami" "al2023" {
  # Use the most recent x86_64 Amazon Linux 2023 image for the producer host.
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

############################################
# Public Subnet + Routing for Producer
############################################

resource "aws_subnet" "producer_public" {
  # This public subnet provides package-download and optional SSH access for EC2.
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.producer_public_subnet_cidr
  availability_zone       = var.producer_public_subnet_az
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.common_name}-producer-public"
  }
}

resource "aws_internet_gateway" "main" {
  # Internet access is required only by the producer host in the public subnet.
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.common_name}-igw"
  }
}

resource "aws_route_table" "producer_public" {
  # Route the producer subnet's internet-bound traffic through the IGW.
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${local.common_name}-producer-public-rt"
  }
}

resource "aws_route_table_association" "producer_public" {
  # Apply the public route table to the producer subnet.
  subnet_id      = aws_subnet.producer_public.id
  route_table_id = aws_route_table.producer_public.id
}

############################################
# Security Group for Producer
############################################

resource "aws_security_group" "producer" {
  # Security group for the EC2 host that runs the Kafka producer tools.
  name        = "${local.common_name}-producer-sg"
  description = "Security Group for producer EC2"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.common_name}-producer-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "producer_ssh" {
  # SSH is optional; restrict producer_ssh_allowed_cidrs in real environments.
  for_each = toset(var.producer_ssh_allowed_cidrs)

  security_group_id = aws_security_group.producer.id
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = each.value
  description       = "Allow SSH"
}

resource "aws_vpc_security_group_egress_rule" "producer_all" {
  # Allows dependency downloads and outbound calls made by the host.
  security_group_id = aws_security_group.producer.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "Allow all outbound traffic"
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_producer" {
  # Allow the producer security group to reach MSK's IAM TLS listener.
  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = aws_security_group.producer.id
  ip_protocol                  = "tcp"
  from_port                    = 9098
  to_port                      = 9098
  description                  = "Allow producer EC2 to MSK IAM listener"
}

############################################
# Producer IAM Role + Instance Profile
############################################

data "aws_iam_policy_document" "producer_assume" {
  # Trust policy for the EC2 instance profile.
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "producer" {
  # Instance role used for IAM-authenticated Kafka operations and SSM access.
  name               = "${local.common_name}-producer-role"
  assume_role_policy = data.aws_iam_policy_document.producer_assume.json
}

locals {
  producer_source_files = {
    "requirements.txt"  = "${path.module}/producer/requirements.txt"
    "config.py"         = "${path.module}/producer/config.py"
    "kafka_utils.py"    = "${path.module}/producer/kafka_utils.py"
    "create_topic.py"   = "${path.module}/producer/create_topic.py"
    "producer.py"       = "${path.module}/producer/producer.py"
    "run_on_ec2.sh"     = "${path.module}/producer/run_on_ec2.sh"
    "README.md"         = "${path.module}/producer/README.md"
  }

  producer_source_prefix = "bootstrap/producer"
}

resource "aws_s3_object" "producer_source" {
  # Keep the bootstrap small by downloading the producer source during launch.
  for_each = local.producer_source_files

  bucket = aws_s3_bucket.firehose.id
  key    = "${local.producer_source_prefix}/${each.key}"
  source = each.value
  etag   = filemd5(each.value)
}

data "aws_iam_policy_document" "producer_msk" {
  # The producer can manage topics and produce or read records across this cluster.
  statement {
    sid    = "MskBootstrap"
    effect = "Allow"

    actions = [
      "kafka:GetBootstrapBrokers",
      "kafka:DescribeCluster",
      "kafka:DescribeClusterV2"
    ]

    resources = [
      aws_msk_serverless_cluster.this.arn
    ]
  }

  statement {
    sid    = "MskDataPlane"
    effect = "Allow"

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DeleteTopic",
      "kafka-cluster:AlterTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData"
    ]

    resources = [
      aws_msk_serverless_cluster.this.arn,
      "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/")}/*",
      "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")}/*"
    ]
  }

  statement {
    sid    = "ProducerSourceDownload"
    effect = "Allow"

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.firehose.arn}/${local.producer_source_prefix}/*"
    ]
  }
}

resource "aws_iam_policy" "producer_msk" {
  # Customer-managed MSK permissions assigned to the producer instance role.
  name   = "${local.common_name}-producer-msk"
  policy = data.aws_iam_policy_document.producer_msk.json
}

resource "aws_iam_role_policy_attachment" "producer_msk" {
  # Bind MSK permissions to the producer role.
  role       = aws_iam_role.producer.name
  policy_arn = aws_iam_policy.producer_msk.arn
}

resource "aws_iam_role_policy_attachment" "producer_ssm" {
  # Enable Session Manager so a key pair is not required for administrative access.
  role       = aws_iam_role.producer.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "producer" {
  # EC2 consumes IAM roles through an instance profile.
  name = "${local.common_name}-producer-profile"
  role = aws_iam_role.producer.name
}

############################################
# Producer EC2
############################################

resource "aws_instance" "producer" {
  # Bootstrap the producer tools and configured MSK endpoint at instance launch.
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.producer_instance_type
  subnet_id                   = aws_subnet.producer_public.id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.producer.id]
  iam_instance_profile        = aws_iam_instance_profile.producer.name
  key_name                    = var.producer_key_name != "" ? var.producer_key_name : null

  user_data_replace_on_change = true
  # Download producer source staged in the private deployment bucket.
  user_data = templatefile("${path.module}/templates/producer_user_data.sh.tftpl", {
    aws_region             = var.aws_region
    bootstrap_servers      = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
    topic_name             = var.topic_name
    producer_source_bucket = aws_s3_bucket.firehose.bucket
    producer_source_prefix = local.producer_source_prefix
    producer_files         = keys(local.producer_source_files)
  })

  # Start only after MSK and the instance's MSK/SSM permissions are available.
  depends_on = [
    aws_msk_serverless_cluster.this,
    aws_iam_role_policy_attachment.producer_msk,
    aws_iam_role_policy_attachment.producer_ssm,
    aws_s3_object.producer_source
  ]

  tags = {
    Name = "${local.common_name}-producer"
  }
}
