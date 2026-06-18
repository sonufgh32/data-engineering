#!/bin/bash

yum update -y
yum install -y httpd

cat > /var/www/html/index.html <<'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Blue Environment</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: Arial, sans-serif;
        }

        body {
            background: linear-gradient(135deg, #007BFF, #00C6FF);
            height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            color: white;
        }

        .card {
            text-align: center;
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            padding: 40px;
            border-radius: 20px;
            width: 600px;
            box-shadow: 0 20px 50px rgba(0,0,0,0.3);
        }

        .status {
            width: 18px;
            height: 18px;
            background: #00ff7f;
            border-radius: 50%;
            display: inline-block;
            animation: pulse 1.5s infinite;
        }

        h1 {
            font-size: 3rem;
            margin-bottom: 20px;
        }

        h2 {
            margin-bottom: 20px;
        }

        .version {
            font-size: 1.5rem;
            margin-top: 15px;
        }

        @keyframes pulse {
            0% { transform: scale(1);}
            50% { transform: scale(1.4);}
            100% { transform: scale(1);}
        }
    </style>
</head>
<body>

<div class="card">
    <h1>🔵 BLUE ENVIRONMENT</h1>
    <h2><span class="status"></span> ACTIVE</h2>

    <div class="version">
        Version: v1.0
    </div>

    <p style="margin-top:20px;">
        Terraform Blue-Green Deployment Demo
    </p>

    <p style="margin-top:10px;">
        Hosted on AWS EC2 + ALB + Auto Scaling
    </p>
</div>

</body>
</html>
EOF

systemctl enable httpd
systemctl restart httpd