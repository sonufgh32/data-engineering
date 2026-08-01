module "vpc" {

  source = "./modules/vpc"

  project_name = var.project_name

}

module "security" {

  source = "./modules/security"

  vpc_id = module.vpc.vpc_id

}

module "iam" {
  source = "./modules/iam"
}

module "s3" {

  source = "./modules/s3"

  project_name = var.project_name

}

moved {
  from = module.automation.aws_s3_object.bootstrap
  to   = aws_s3_object.bootstrap
}

# This artifact must exist before EMR starts its bootstrap action.
resource "aws_s3_object" "bootstrap" {

  bucket = module.s3.scripts_bucket

  key = "bootstrap/bootstrap.sh"

  source = "${path.root}/../scripts/bootstrap/bootstrap.sh"

  source_hash = filemd5("${path.root}/../scripts/bootstrap/bootstrap.sh")

}

module "msk" {

  source = "./modules/msk"

  project_name = var.project_name

  private_subnets = module.vpc.private_subnets

  security_group_id = module.security.msk_security_group

}

module "emr" {

  source = "./modules/emr"

  # EMR validates network connectivity and assumes its roles during creation.
  # Wait for private routing and all role policies to be fully provisioned.
  depends_on = [module.vpc, module.iam, aws_s3_object.bootstrap]

  project_name = var.project_name

  private_subnets = module.vpc.private_subnets

  master_security_group = module.security.emr_master_security_group

  core_security_group = module.security.emr_core_security_group

  service_access_security_group = module.security.emr_service_access_security_group

  service_role = module.iam.emr_service_role

  instance_profile = module.iam.instance_profile_name

  logs_bucket = module.s3.logs_bucket

  scripts_bucket = module.s3.scripts_bucket

}

module "automation" {

  source = "./modules/automation"

  scripts_bucket = module.s3.scripts_bucket

  output_bucket = module.s3.output_bucket

  emr_cluster_id = module.emr.cluster_id

  bootstrap_servers = module.msk.bootstrap_brokers

}