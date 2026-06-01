# String Variable
variable "environment" {
    description = "Deployment environment"
    type        = string
    default     = "dev"
}

# Number Variable
variable "instance_count" {
    description = "Number of instances"
    type        = number
    default     = 2
}

# Boolean Variable
variable "enable_monitoring" {
    description = "Enable monitoring"
    type        = bool
    default     = true
}

# List Variable
variable "availability_zones" {
    description = "List of AZs"
    type        = list(string)
    default     = [
        "us-east-1a",
        "us-east-1b"
    ]
}

# Set Variable
variable "allowed_ports" {
    description = "Allowed ports"
    type        = set(number)
    default     = [22, 80, 443]
}

# Map Variable
variable "tags" {
    description = "Resource tags"
    type        = map(string)
    default = {
        Owner       = "DevOps"
        Environment = "Development"
    }
}

# Object Variable
variable "ec2_config" {
    description = "EC2 Configuration"
    
    type = object({
        instance_type = string
        ami_id        = string
        root_volume   = number
    })
    
    default = {
        instance_type = "t3.micro"
        ami_id        = "ami-12345678"
        root_volume   = 20
    }
}

# Tuple Variable
variable "server_info" {
    description = "Server Information"

    type = tuple([
        string,
        number,
        bool
    ])

    default = [
        "web-server",
        8080,
        true
    ]
}

# List of Objects
variable "users" {
    description = "Application users"

    type = list(object({
        name = string
        role = string
    }))

    default = [{
        name = "john"
        role = "admin"
    }, {
        name = "alice"
        role = "developer"
    }]
}

# Optional Attributes (Terraform 1.3+)
variable "application" {
    type = object({
        name     = string
        version  = optional(string, "1.0")
        replicas = optional(number, 2)
    })

    default = {
        name = "my-app"
    }
}


#########################################

terraform {
    required_version = ">= 1.3.0"
}

output "environment" {
    value = var.environment
}

output "instance_count" {
    value = var.instance_count
}

output "enable_monitoring" {
    value = var.enable_monitoring
}

output "availability_zones" {
    value = var.availability_zones
}

output "allowed_ports" {
    value = var.allowed_ports
}

output "tags" {
    value = var.tags
}

output "ec2_instance_type" {
    value = var.ec2_config.instance_type
}

output "server_name" {
    value = var.server_info[0]
}

output "server_port" {
    value = var.server_info[1]
}

output "users" {
    value = var.users
}

output "application" {
    value = var.application
}