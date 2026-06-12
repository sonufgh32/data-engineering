output "lambda_name" {
  value = aws_lambda_function.app.function_name
}

output "lambda_arn" {
  value = aws_lambda_function.app.arn
}

output "region" {
  value = var.aws_region
}