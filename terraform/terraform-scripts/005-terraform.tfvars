environment       = "prod"
instance_count    = 5
enable_monitoring = true

availability_zones = [
    "us-east-1a",
    "us-east-1b",
    "us-east-1c"
]

allowed_ports = [22, 80, 443, 8080]

tags = {
    Owner       = "Shiv"
    Environment = "Production"
}

ec2_config = {
    instance_type = "t3.large"
    ami_id        = "ami-0abcdef1234567890"
    root_volume   = 50
}

server_info = [
    "api-server",
    9000,
    true
]

users = [
    {
        name = "shiv"
        role = "admin"
    },
    {
        name = "bob"
        role = "developer"
    }
]

application = {
    name     = "customer-api"
    version  = "2.0"
    replicas = 3
}