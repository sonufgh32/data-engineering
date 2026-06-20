data "aws_ami" "amazon_linux" {
  most_recent = true

  owners = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*"]
  }
}

resource "aws_security_group" "vault" {
  name_prefix = "vault-sg"

  ingress {
    from_port   = var.ssh_port
    to_port     = var.ssh_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr
  }

  ingress {
    from_port   = var.vault_port
    to_port     = var.vault_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "vault_role" {
  name = "vault-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Action = "sts:AssumeRole"

      Effect = "Allow"

      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_instance_profile" "vault_profile" {
  name = "vault-profile"
  role = aws_iam_role.vault_role.name
}

resource "aws_instance" "vault" {

  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type

  subnet_id              = var.subnet_id

  vpc_security_group_ids = [
    aws_security_group.vault.id
  ]

  key_name               = var.key_name

  iam_instance_profile   = aws_iam_instance_profile.vault_profile.name

  user_data = file("${path.module}/user_data.sh")

  tags = {
    Name = "vault-server"
  }
}