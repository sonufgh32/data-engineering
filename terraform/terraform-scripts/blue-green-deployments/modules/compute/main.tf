resource "aws_security_group" "ec2" {

  name = "blue-green-ec2"

  vpc_id = var.vpc_id

  ingress {
    from_port = 80
    to_port   = 80
    protocol  = "tcp"

    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {

    from_port   = 0
    to_port     = 0
    protocol    = "-1"

    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_launch_template" "blue" {

  name_prefix = "blue"

  image_id      = var.ami_id
  instance_type = var.instance_type

  vpc_security_group_ids = [
    aws_security_group.ec2.id
  ]

  user_data = base64encode(
    file("${path.root}/user-data/blue.sh")
  )
}

resource "aws_launch_template" "green" {

  name_prefix = "green"

  image_id      = var.ami_id
  instance_type = var.instance_type

  vpc_security_group_ids = [
    aws_security_group.ec2.id
  ]

  user_data = base64encode(
    file("${path.root}/user-data/green.sh")
  )
}

resource "aws_autoscaling_group" "blue" {

  name = "blue-asg"

  desired_capacity = 2
  min_size         = 2
  max_size         = 4

  vpc_zone_identifier = var.public_subnets

  target_group_arns = [
    var.blue_target_group
  ]

  launch_template {
    id      = aws_launch_template.blue.id
    version = "$Latest"
  }
}

resource "aws_autoscaling_group" "green" {

  name = "green-asg"

  desired_capacity = 2
  min_size         = 2
  max_size         = 4

  vpc_zone_identifier = var.public_subnets

  target_group_arns = [
    var.green_target_group
  ]

  launch_template {
    id      = aws_launch_template.green.id
    version = "$Latest"
  }
}