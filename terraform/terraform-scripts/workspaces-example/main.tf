data "aws_ami" "amazon_linux" {
  most_recent = true

  owners = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*"]
  }
}

resource "aws_vpc" "main" {

  cidr_block = local.current_env.cidr

  tags = {
    Name = "${terraform.workspace}-vpc"
  }
}

resource "aws_subnet" "public" {

  vpc_id = aws_vpc.main.id

  cidr_block = local.current_env.cidr

  availability_zone = "ap-south-1a"

  map_public_ip_on_launch = true

  tags = {
    Name = "${terraform.workspace}-public-subnet"
  }
}

resource "aws_security_group" "web" {

  name   = "${terraform.workspace}-web-sg"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "web" {

  count = var.instance_count[terraform.workspace]

  ami           = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type[terraform.workspace]

  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.web.id]

  user_data = templatefile("${path.module}/userdata.sh", {
    workspace = terraform.workspace
  })

  tags = {
    Name = "${terraform.workspace}-web-${count.index + 1}"
  }
}

# IGW
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${terraform.workspace}-igw"
  }
}

resource "aws_route_table" "public" {

  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"

    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${terraform.workspace}-public-rt"
  }
}

resource "aws_route_table_association" "public" {

  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}