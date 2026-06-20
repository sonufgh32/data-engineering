output "vault_public_ip" {
  value = module.vault.public_ip
}

output "vault_public_dns" {
  value = module.vault.public_dns
}

output "vault_url" {
  value = "http://${module.vault.public_ip}:8200"
}