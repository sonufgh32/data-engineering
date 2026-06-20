resource "aws_eks_node_group" "this" {

  cluster_name = aws_eks_cluster.this.name

  node_group_name = "worker-nodes"

  node_role_arn = aws_iam_role.node_group.arn

  subnet_ids = aws_subnet.private[*].id

  instance_types = var.instance_types

  scaling_config {

    desired_size = var.desired_size

    min_size = var.min_size

    max_size = var.max_size
  }

  depends_on = [
    aws_eks_addon.vpc_cni,
    aws_eks_addon.kube_proxy
  ]
}