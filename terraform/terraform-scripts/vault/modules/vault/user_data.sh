#!/bin/bash

yum update -y

yum install -y yum-utils

yum-config-manager --add-repo \
https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo

yum install -y vault

mkdir -p /opt/vault/data

cat <<EOF > /etc/vault.d/vault.hcl

ui = true

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = 1
}

storage "file" {
  path = "/opt/vault/data"
}

api_addr = "http://0.0.0.0:8200"

disable_mlock = true

EOF

systemctl enable vault

systemctl start vault