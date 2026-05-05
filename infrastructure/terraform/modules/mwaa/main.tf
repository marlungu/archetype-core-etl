resource "aws_mwaa_environment" "main" {
  name               = "${var.project_name}-${var.environment}"
  airflow_version    = var.airflow_version
  environment_class  = var.environment_class
  execution_role_arn = var.execution_role_arn
  source_bucket_arn  = var.dags_bucket_arn
  dag_s3_path        = "dags/pipelines/"
  requirements_s3_path = "requirements.txt"

  network_configuration {
    security_group_ids = var.security_group_ids
    subnet_ids         = slice(var.private_subnet_ids, 0, 2)
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  max_workers           = 2
  min_workers           = 1
  webserver_access_mode = var.webserver_access_mode

  airflow_configuration_options = {
    "core.load_examples" = "false"
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-mwaa"
  }
}
