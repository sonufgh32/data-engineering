terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

# Get latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux" {
  most_recent = true

  owners = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

# VPC
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true

  tags = {
    Name = "webserver-vpc"
  }
}

# Public Subnet
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "ap-south-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "public-subnet"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "webserver-igw"
  }
}

# Route Table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "public-route-table"
  }
}

# Route Table Association
resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Security Group
resource "aws_security_group" "web_sg" {
  name        = "webserver-sg"
  description = "Allow HTTP HTTPS SSH"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"

    # Replace with your IP
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "webserver-sg"
  }
}

# EC2 Instance
resource "aws_instance" "webserver" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = "t3.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.web_sg.id]
  key_name               = "ClusterVPCKeyPair"

  # Example provisioners (use only when needed)
  provisioner "local-exec" {
    command    = "echo 'Provisioning EC2 instance ${self.id}' >> provisioner.log"
    when       = create
    on_failure = continue
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'Hello from remote-exec provisioner' > /tmp/provisioner-demo.txt",
      "sudo dnf install -y curl",
      "echo 'Provisioner completed successfully'"
    ]

    when       = create
    on_failure = fail

    connection {
      type        = "ssh"
      user        = "ec2-user"
      private_key = file("C:/Users/sonuf/Downloads/Private_Keys/ClusterVPCKeyPair.pem")
      host        = self.public_ip
    }
  }

  provisioner "local-exec" {
    command    = "echo 'Destroying EC2 instance ${self.id}' >> destroy.log"
    when       = destroy
    on_failure = continue
  }

  user_data = <<-EOF
              #!/bin/bash
              dnf update -y
              dnf install -y httpd

              systemctl enable httpd
              systemctl start httpd

              cat <<HTML > /var/www/html/index.html
              <html>
              <head>
                  <title>Terraform Web Server</title>
              </head>
              <body>
                  <h1>Hello from Terraform!</h1>
                  <h2>Running on EC2 in ap-south-1</h2>
              </body>
              </html>
              HTML
              EOF

  tags = {
    Name = "terraform-webserver"
  }
}

# Useful Terraform commands
# terraform fmt
# terraform validate
# terraform plan
# terraform apply
# terraform destroy

# Outputs
output "instance_id" {
  value = aws_instance.webserver.id
}

output "instance_public_ip" {
  value = aws_instance.webserver.public_ip
}

output "instance_public_dns" {
  value = aws_instance.webserver.public_dns
}

output "instance_ami_id" {
  value = aws_instance.webserver.ami
}

output "vpc_id" {
  value = aws_vpc.main.id
}

output "subnet_id" {
  value = aws_subnet.public.id
}

output "security_group_id" {
  value = aws_security_group.web_sg.id
}

output "website_url" {
  value = "http://${aws_instance.webserver.public_ip}"
}

output "deployment_summary" {
  value = "EC2 instance ${aws_instance.webserver.id} is running at ${aws_instance.webserver.public_ip}"
}