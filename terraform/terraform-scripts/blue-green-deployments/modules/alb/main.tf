resource "aws_security_group" "alb" {

  name = "blue-green-alb"

  vpc_id = var.vpc_id

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

resource "aws_lb" "main" {

  name               = "blue-green-alb"
  load_balancer_type = "application"

  subnets         = var.public_subnets
  security_groups = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "blue" {

  name     = "blue-tg"
  port     = 80
  protocol = "HTTP"

  vpc_id = var.vpc_id

  health_check {
    path = "/"
  }
}

resource "aws_lb_target_group" "green" {

  name     = "green-tg"
  port     = 80
  protocol = "HTTP"

  vpc_id = var.vpc_id

  health_check {
    path = "/"
  }
}

resource "aws_lb_listener" "http" {

  load_balancer_arn = aws_lb.main.arn

  port     = 80
  protocol = "HTTP"

  default_action {

    type = "forward"

    target_group_arn = (
      var.active_environment == "blue"
      ? aws_lb_target_group.blue.arn
      : aws_lb_target_group.green.arn
    )
  }
}