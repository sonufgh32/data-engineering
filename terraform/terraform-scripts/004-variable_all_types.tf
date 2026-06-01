terraform {
  required_version = ">= 1.5.0"
}

##################################
# VARIABLES
##################################

variable "project_name" {
    type    = string
    default = "terraform-demo"
}

variable "instance_count" {
    type    = number
    default = 3
}

variable "enable_backup" {
    type    = bool
    default = true
}

variable "subnets" {
    type    = list(string)
    default = [
        "subnet-1",
        "subnet-2",
        "subnet-3"
    ]
}

variable "allowed_ports" {
    type    = set(number)
    default = [22, 80, 443]
}

variable "tags" {
    type = map(string)
    default = {
        Environment = "dev"
        Owner       = "Shiv"
    }
}

variable "server_config" {
    type = object({
        instance_type = string
        volume_size   = number
    })

    default = {
        instance_type = "t3.micro"
        volume_size   = 20
    }
}

variable "app_info" {
    type = tuple([
        string,
        number,
        bool
    ])

    default = [
        "customer-api",
        8080,
        true
    ]
}

variable "users" {
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

##################################
# OUTPUTS
##################################

output "project_name" {
    value = var.project_name
}

output "instance_count" {
    value = var.instance_count
}

output "enable_backup" {
    value = var.enable_backup
}

output "first_subnet" {
    value = var.subnets[0]
}

output "allowed_ports" {
    value = var.allowed_ports
}

output "owner_tag" {
    value = var.tags["Owner"]
}

output "instance_type" {
    value = var.server_config.instance_type
}

output "app_name" {
    value = var.app_info[0]
}

output "app_port" {
    value = var.app_info[1]
}

output "first_user" {
    value = var.users[0].name
}