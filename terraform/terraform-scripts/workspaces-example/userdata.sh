#!/bin/bash

sudo yum update -y
sudo yum install -y httpd

sudo systemctl enable httpd
sudo systemctl start httpd

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
HOSTNAME=$(hostname)
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

cat <<EOF > /var/www/html/index.html
<!DOCTYPE html>
<html>
<head>
    <title>Terraform Workspace Demo</title>

    <style>
        *{
            margin:0;
            padding:0;
            box-sizing:border-box;
            font-family:Arial, sans-serif;
        }

        body{
            height:100vh;
            background:linear-gradient(135deg,#1e3c72,#2a5298);
            display:flex;
            justify-content:center;
            align-items:center;
        }

        .card{
            width:700px;
            background:white;
            border-radius:20px;
            padding:40px;
            box-shadow:0 15px 40px rgba(0,0,0,0.3);
            text-align:center;
        }

        .title{
            font-size:38px;
            color:#333;
            margin-bottom:20px;
        }

        .workspace{
            display:inline-block;
            background:#28a745;
            color:white;
            font-size:28px;
            font-weight:bold;
            padding:12px 30px;
            border-radius:50px;
            margin-bottom:30px;
        }

        .info{
            display:grid;
            grid-template-columns:1fr 1fr;
            gap:20px;
            margin-top:20px;
        }

        .box{
            background:#f5f5f5;
            padding:20px;
            border-radius:12px;
            box-shadow:0 5px 10px rgba(0,0,0,0.1);
        }

        .label{
            font-size:14px;
            color:#777;
        }

        .value{
            margin-top:8px;
            font-size:20px;
            font-weight:bold;
            color:#222;
        }

        .footer{
            margin-top:30px;
            color:#666;
            font-size:14px;
        }

        .status{
            margin-top:20px;
            color:#28a745;
            font-size:18px;
            font-weight:bold;
        }
    </style>
</head>
<body>

<div class="card">

    <h1 class="title">Terraform Workspace Demo</h1>

    <div class="workspace">
        ${workspace}
    </div>

    <div class="status">
        Infrastructure Successfully Deployed
    </div>

    <div class="info">

        <div class="box">
            <div class="label">Workspace</div>
            <div class="value">${workspace}</div>
        </div>

        <div class="box">
            <div class="label">Instance ID</div>
            <div class="value">$${{INSTANCE_ID}}</div>
        </div>

        <div class="box">
            <div class="label">Hostname</div>
            <div class="value">$${{HOSTNAME}}</div>
        </div>

        <div class="box">
            <div class="label">Private IP</div>
            <div class="value">$${{PRIVATE_IP}}</div>
        </div>

    </div>

    <div class="footer">
        AWS EC2 + Terraform Workspaces + Apache
    </div>

</div>

</body>
</html>
EOF

sudo systemctl restart httpd