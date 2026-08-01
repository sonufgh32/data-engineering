output "msk_security_group" {

  value = aws_security_group.msk.id

}

output "emr_master_security_group" {

  value = aws_security_group.emr_master.id

}

output "emr_core_security_group" {

  value = aws_security_group.emr_core.id

}

output "emr_service_access_security_group" {

  value = aws_security_group.emr_service_access.id

}