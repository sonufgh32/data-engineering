-- Example 1: Create an external schema for federated query from MySQL
CREATE EXTERNAL SCHEMA federated_query
FROM MYSQL
DATABASE 'demo'
URI 'redshift-rds.czwu8km424bq.ap-south-1.rds.amazonaws.com'
IAM_ROLE 'arn:aws:iam::741448939728:role/Redshift-Federated-Query-Role'
SECRET_ARN 'arn:aws:secretsmanager:ap-south-1:741448939728:secret:demo/rds/mysql-QpCNgr';

