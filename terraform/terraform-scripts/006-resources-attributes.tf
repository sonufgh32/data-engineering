terraform {
    required_providers {
        local = {
            source  = "hashicorp/local"
            version = "~> 2.5"
        }
    }
}

#################################
# Resource 1
#################################

resource "local_file" "config" {
    filename = "config.txt"
    content  = "Application Environment: DEV"
}

#################################
# Resource 2
# Implicit Dependency
#################################

resource "local_file" "application" {
    filename = "application.txt"

    # Using an attribute from local_file.config
    # Creates an implicit dependency
    content = <<EOT
    Application Started

    Config File:
    ${local_file.config.content}
    EOT
}

#################################
# Resource 3
# Explicit Dependency
#################################

resource "local_file" "deployment_log" {
    filename = "deployment.log"
    content  = "Deployment completed successfully"

    depends_on = [
        local_file.config,
        local_file.application
    ]
}

#################################
# Outputs
#################################

output "config_file_name" {
    value = local_file.config.filename
}

output "application_file_name" {
    value = local_file.application.filename
}

output "deployment_log_name" {
    value = local_file.deployment_log.filename
}

output "config_content" {
    value = local_file.config.content
}