# EKS cluster and a managed node group in the private subnets.

resource "aws_security_group" "nodes" {
  name_prefix = "${local.name}-nodes-"
  description = "Chronos EKS worker nodes"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name                                  = "${local.name}-nodes"
    "kubernetes.io/cluster/${local.name}" = "owned"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Pods talk to each other freely inside the cluster; policy between workloads is
# enforced by Kubernetes NetworkPolicy, not by security groups, because the unit
# that matters there is the pod rather than the node.
resource "aws_vpc_security_group_ingress_rule" "nodes_self" {
  security_group_id            = aws_security_group.nodes.id
  description                  = "Node to node"
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.nodes.id
}

resource "aws_vpc_security_group_ingress_rule" "nodes_from_control_plane" {
  security_group_id            = aws_security_group.nodes.id
  description                  = "Control plane to kubelet and webhooks"
  from_port                    = 1025
  to_port                      = 65535
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.cluster.id
}

resource "aws_vpc_security_group_egress_rule" "nodes_all" {
  security_group_id = aws_security_group.nodes.id
  description       = "Node egress"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_security_group" "cluster" {
  name_prefix = "${local.name}-cluster-"
  description = "Chronos EKS control plane"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-cluster" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "cluster_from_nodes" {
  security_group_id            = aws_security_group.cluster.id
  description                  = "Kubelet to API server"
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.nodes.id
}

resource "aws_vpc_security_group_egress_rule" "cluster_all" {
  security_group_id = aws_security_group.cluster.id
  description       = "Control plane egress"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# ---------------------------------------------------------------------------
# Cluster IAM
# ---------------------------------------------------------------------------

resource "aws_iam_role" "cluster" {
  name_prefix = "${local.name}-cluster-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
  ])

  role       = aws_iam_role.cluster.name
  policy_arn = each.value
}

# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------

resource "aws_kms_key" "eks" {
  description             = "Chronos ${var.environment} EKS secret envelope encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "eks" {
  name          = "alias/${local.name}-eks"
  target_key_id = aws_kms_key.eks.key_id
}

resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${local.name}/cluster"
  retention_in_days = 30
}

resource "aws_eks_cluster" "main" {
  name     = local.name
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version

  # API access via IAM entries rather than the aws-auth ConfigMap: access is then
  # a Terraform-managed resource that can be reviewed and revoked, instead of a
  # hand-edited ConfigMap nobody remembers changing.
  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    subnet_ids              = aws_subnet.private[*].id
    security_group_ids      = [aws_security_group.cluster.id]
    endpoint_private_access = true

    # Public access only if a caller is named. Default is no public endpoint at
    # all, so reaching the API requires being inside the VPC.
    endpoint_public_access = length(var.cluster_endpoint_public_access_cidrs) > 0
    public_access_cidrs    = var.cluster_endpoint_public_access_cidrs
  }

  # Encrypt Kubernetes Secrets at rest with a customer-managed key, so etcd
  # alone is not enough to read them.
  encryption_config {
    provider {
      key_arn = aws_kms_key.eks.arn
    }
    resources = ["secrets"]
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  depends_on = [
    aws_iam_role_policy_attachment.cluster,
    aws_cloudwatch_log_group.cluster,
  ]
}

# IRSA: lets a Kubernetes service account assume an IAM role directly, so no
# pod needs static credentials and no node role has to carry the union of every
# workload's permissions.
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}

# ---------------------------------------------------------------------------
# Node group
# ---------------------------------------------------------------------------

resource "aws_iam_role" "nodes" {
  name_prefix = "${local.name}-nodes-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# The node role carries only what the kubelet and CNI need. Application
# permissions go through IRSA instead, so a compromised pod does not inherit
# every permission every other pod on that node requires.
resource "aws_iam_role_policy_attachment" "nodes" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
  ])

  role       = aws_iam_role.nodes.name
  policy_arn = each.value
}

resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${local.name}-general"
  node_role_arn   = aws_iam_role.nodes.arn
  subnet_ids      = aws_subnet.private[*].id

  instance_types = var.node_instance_types
  capacity_type  = "ON_DEMAND"

  scaling_config {
    min_size     = var.node_group_min_size
    max_size     = var.node_group_max_size
    desired_size = var.node_group_desired_size
  }

  # Surge to a whole extra node during a rollout so pods have somewhere to land
  # before the old ones are drained.
  update_config {
    max_unavailable_percentage = 33
  }

  labels = {
    "chronos.io/pool" = "general"
  }

  lifecycle {
    # desired_size drifts as the cluster autoscaler works; Terraform must not
    # fight it on every plan.
    ignore_changes = [scaling_config[0].desired_size]
  }

  depends_on = [aws_iam_role_policy_attachment.nodes]
}

# ---------------------------------------------------------------------------
# Add-ons
# ---------------------------------------------------------------------------

# metrics-server is not an EKS add-on, so the HPA's CPU metrics come from a
# Helm-installed metrics-server (see deploy/k8s/README). These three are the
# baseline the cluster cannot function without.
resource "aws_eks_addon" "core" {
  for_each = {
    vpc-cni    = {}
    coredns    = {}
    kube-proxy = {}
  }

  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = each.key
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "PRESERVE"

  depends_on = [aws_eks_node_group.main]
}
