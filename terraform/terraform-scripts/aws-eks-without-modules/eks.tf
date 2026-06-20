resource "aws_security_group" "eks" {

  name_prefix = "${var.cluster_name}-eks"

  vpc_id = aws_vpc.this.id
}

resource "aws_eks_cluster" "this" {

  name = var.cluster_name

  version = var.cluster_version

  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {

    subnet_ids = concat(
      aws_subnet.public[*].id,
      aws_subnet.private[*].id
    )

    endpoint_public_access = true
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster_policy
  ]
}