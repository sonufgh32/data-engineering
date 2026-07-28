############################################
# AWS Context
############################################
# These data sources make the active account, Region, and partition available
# for ARNs and managed-policy references without hard-coding AWS identifiers.
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}