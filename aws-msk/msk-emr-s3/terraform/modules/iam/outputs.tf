output "emr_service_role" {

  value = aws_iam_role.emr_service_role.arn

}

output "emr_ec2_role" {

  value = aws_iam_role.emr_ec2_role.arn

}

output "instance_profile" {

  value = aws_iam_instance_profile.emr_profile.arn

}

output "instance_profile_name" {

  value = aws_iam_instance_profile.emr_profile.name

}