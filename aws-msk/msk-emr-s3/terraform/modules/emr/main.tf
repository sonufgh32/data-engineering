resource "aws_emr_cluster" "this" {

  name = "${var.project_name}-cluster"

  # EMR 7.13 is supported through April 2028 and bundles Spark 3.5.6.
  release_label = "emr-7.13.0"

  applications = [
    "Spark",
    "Hadoop",
    "Hive",
    "Livy"
  ]

  service_role = var.service_role

  log_uri = "s3://${var.logs_bucket}/emr-logs/"

  keep_job_flow_alive_when_no_steps = true

  termination_protection = false

  ec2_attributes {

    subnet_id = var.private_subnets[0]

    instance_profile = var.instance_profile

    emr_managed_master_security_group = var.master_security_group

    emr_managed_slave_security_group = var.core_security_group

    service_access_security_group = var.service_access_security_group

  }

  master_instance_group {

    # Single-node cluster for development and low-volume streaming tests.
    # EMR 7.1 rejects smaller t3.medium and m5.large types in this configuration.
    instance_type = "m5.xlarge"

    instance_count = 1

  }

  ebs_root_volume_size = 50


  autoscaling_role = var.service_role

  # bootstrap_action {

  #   name = "Bootstrap"

  #   path = "s3://${var.scripts_bucket}/bootstrap/bootstrap.sh"

  # }
  bootstrap_action {

    name = "Install Kafka Libraries"

    path = "s3://${var.scripts_bucket}/bootstrap/bootstrap.sh"

  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"

      Properties = {

        "spark.sql.catalogImplementation" = "hive"

        # Bootstrap installs these JARs on every EMR node for Kafka/MSK IAM access.
        "spark.jars" = join(",", [
          "/home/hadoop/spark-jars/spark-sql-kafka-0-10_2.12-3.5.6.jar",
          "/home/hadoop/spark-jars/kafka-clients-3.7.0.jar",
          "/home/hadoop/spark-jars/aws-msk-iam-auth.jar",
        ])

      }

    }
  ])

  tags = {

    Environment = "Development"

    Project = var.project_name

    # EMR propagates this tag to launched EC2 resources required by its v2 role policy.
    "for-use-with-amazon-emr-managed-policies" = "true"

  }

}