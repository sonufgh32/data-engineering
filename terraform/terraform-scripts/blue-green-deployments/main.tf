module "alb" {
  source = "./modules/alb"

  vpc_id             = var.vpc_id
  public_subnets     = var.public_subnets
  active_environment = var.active_environment
}

module "compute" {
  source = "./modules/compute"

  vpc_id         = var.vpc_id
  public_subnets = var.public_subnets
  ami_id         = var.ami_id
  instance_type  = var.instance_type

  blue_target_group  = module.alb.blue_target_group_arn
  green_target_group = module.alb.green_target_group_arn
}