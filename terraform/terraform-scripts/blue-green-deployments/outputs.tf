output "alb_dns_name" {
  value = module.alb.alb_dns_name
}

output "active_environment" {
  value = var.active_environment
}