resource "aws_s3_object" "etl" {

  bucket = var.scripts_bucket

  key = "etl/streaming_job.py"

  source = "${path.root}/../etl/streaming_job.py"

}

resource "aws_s3_object" "config" {

  bucket = var.scripts_bucket

  key = "etl/config.py"

  source = "${path.root}/../etl/config.py"

}

resource "aws_s3_object" "schema" {

  bucket = var.scripts_bucket

  key = "etl/schema.py"

  source = "${path.root}/../etl/schema.py"

}

resource "aws_s3_object" "utils" {

  bucket = var.scripts_bucket

  key = "etl/utils.py"

  source = "${path.root}/../etl/utils.py"
}

resource "null_resource" "submit_job" {

  depends_on = [
    aws_s3_object.etl,
    aws_s3_object.config,
    aws_s3_object.schema,
    aws_s3_object.utils,
  ]

  provisioner "local-exec" {

    command = <<EOT

aws emr add-steps \
--cluster-id ${var.emr_cluster_id} \
--steps Type=Spark,\
Name=Streaming,\
ActionOnFailure=CONTINUE,\
Args=[--conf,spark.msk-emr-s3.bootstrap-servers=${var.bootstrap_servers},--conf,spark.msk-emr-s3.topic-name=${var.topic_name},--conf,spark.msk-emr-s3.output-path=s3://${var.output_bucket}/parquet/,--conf,spark.msk-emr-s3.checkpoint-path=s3://${var.output_bucket}/checkpoints/,--py-files,s3://${var.scripts_bucket}/etl/config.py\,s3://${var.scripts_bucket}/etl/schema.py\,s3://${var.scripts_bucket}/etl/utils.py,s3://${var.scripts_bucket}/etl/streaming_job.py]

EOT

  }

}