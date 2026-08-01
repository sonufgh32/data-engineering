output "scripts_bucket" {

  value = aws_s3_bucket.scripts.bucket

}

output "output_bucket" {

  value = aws_s3_bucket.output.bucket

}

output "logs_bucket" {

  value = aws_s3_bucket.logs.bucket

}