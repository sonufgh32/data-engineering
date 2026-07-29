############################################
# Firehose CSV Transformation Lambda
############################################

data "archive_file" "firehose_csv_transformer" {
  type        = "zip"
  source_file = "${path.module}/lambda/firehose_csv_transformer.py"
  output_path = "${path.module}/.terraform/firehose_csv_transformer.zip"
}

data "aws_iam_policy_document" "firehose_csv_transformer_assume" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "firehose_csv_transformer" {
  name               = "${local.common_name}-firehose-csv-transformer-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_csv_transformer_assume.json
}

data "aws_iam_policy_document" "firehose_csv_transformer" {
  statement {
    sid    = "WriteLogs"
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose_csv_transformer" {
  name   = "${local.common_name}-firehose-csv-transformer"
  role   = aws_iam_role.firehose_csv_transformer.id
  policy = data.aws_iam_policy_document.firehose_csv_transformer.json
}

resource "aws_lambda_function" "firehose_csv_transformer" {
  function_name = "${local.common_name}-firehose-csv-transformer"
  description   = "Transforms Firehose JSON records into CSV rows"
  role          = aws_iam_role.firehose_csv_transformer.arn
  handler       = "firehose_csv_transformer.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60

  filename         = data.archive_file.firehose_csv_transformer.output_path
  source_code_hash = data.archive_file.firehose_csv_transformer.output_base64sha256

  depends_on = [aws_iam_role_policy.firehose_csv_transformer]
}

resource "aws_lambda_permission" "allow_firehose_csv_transformer" {
  statement_id   = "AllowFirehoseInvocation"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.firehose_csv_transformer.function_name
  principal      = "firehose.amazonaws.com"
  source_account = data.aws_caller_identity.current.account_id
}