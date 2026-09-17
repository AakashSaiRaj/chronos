# IAM: least privilege via IRSA, plus a CI/CD role assumed through GitHub OIDC.
#
# The principle applied throughout: every policy names the exact resource ARN it
# needs. No wildcards on resources, and no permission granted because it might be
# convenient later.

data "aws_caller_identity" "current" {}

locals {
  oidc_provider_url = replace(aws_iam_openid_connect_provider.eks.url, "https://", "")

  # The service accounts that exist in the cluster. Each gets its own role, so a
  # compromised worker cannot read anything the API can and vice versa.
  service_accounts = {
    server = "chronos-server"
    worker = "chronos-worker"
  }
}

# ---------------------------------------------------------------------------
# Server role: reads the database secret
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "server_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    # Scoped to one namespace and one service account. A pod in another namespace
    # presenting a token from this same cluster cannot assume the role.
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${local.service_accounts.server}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "server" {
  name_prefix        = "${local.name}-server-"
  description        = "Chronos API and scheduler pods"
  assume_role_policy = data.aws_iam_policy_document.server_assume.json
}

data "aws_iam_policy_document" "server" {
  statement {
    sid    = "ReadDatabaseSecret"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    # One specific secret, named. Not secretsmanager:* on all resources.
    resources = [aws_secretsmanager_secret.database.arn]
  }

  statement {
    sid       = "DecryptDatabaseSecret"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.database.arn]

    # Decrypt only in the context of reading that secret, so the key cannot be
    # used to decrypt anything else that happens to share it.
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "server" {
  name_prefix = "chronos-server-"
  role        = aws_iam_role.server.id
  policy      = data.aws_iam_policy_document.server.json
}

# ---------------------------------------------------------------------------
# Worker role: deliberately almost empty
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "worker_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${local.service_accounts.worker}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

# Workers reach the control plane only over HTTP and hold no AWS credentials,
# because they need none: no database access, no secret access, nothing. The role
# exists so the service account has a stable identity for audit logs, and so that
# granting a worker something later is an explicit, reviewable change.
resource "aws_iam_role" "worker" {
  name_prefix        = "${local.name}-worker-"
  description        = "Chronos worker pods (no AWS permissions by design)"
  assume_role_policy = data.aws_iam_policy_document.worker_assume.json
}

resource "aws_iam_role_policy" "worker_sqs" {
  count = var.enable_sqs ? 1 : 0

  name_prefix = "chronos-worker-sqs-"
  role        = aws_iam_role.worker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "PublishDeadLetterNotifications"
      Effect = "Allow"
      Action = ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"]
      Resource = [
        aws_sqs_queue.main[0].arn,
      ]
    }]
  })
}

# ---------------------------------------------------------------------------
# CI/CD role, assumed from GitHub Actions via OIDC
# ---------------------------------------------------------------------------

# Federated OIDC rather than a stored access key: nothing long-lived exists to
# leak, and the trust policy pins which repository may assume the role.
resource "aws_iam_openid_connect_provider" "github" {
  count = var.github_repository == "" ? 0 : 1

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # GitHub's OIDC thumbprint. AWS ignores it for this well-known provider but the
  # API still requires a value.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "cicd_assume" {
  count = var.github_repository == "" ? 0 : 1

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Only this repository, and only from its default branch or a tag. A pull
    # request from a fork cannot assume the deploy role.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repository}:ref:refs/heads/mainline",
        "repo:${var.github_repository}:ref:refs/tags/*",
        "repo:${var.github_repository}:environment:${var.environment}",
      ]
    }
  }
}

resource "aws_iam_role" "cicd" {
  count = var.github_repository == "" ? 0 : 1

  name_prefix          = "${local.name}-cicd-"
  description          = "GitHub Actions deploy role for ${var.github_repository}"
  assume_role_policy   = data.aws_iam_policy_document.cicd_assume[0].json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "cicd" {
  count = var.github_repository == "" ? 0 : 1

  statement {
    sid       = "GetECRAuthToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # this action does not support resource scoping
  }

  statement {
    sid    = "PushImages"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecr:BatchGetImage",
      "ecr:DescribeImages",
    ]
    resources = [aws_ecr_repository.chronos.arn]
  }

  # Enough to fetch a kubeconfig, nothing more. Authorization inside the cluster
  # is a separate EKS access entry, so this role cannot grant itself Kubernetes
  # permissions by editing IAM.
  statement {
    sid       = "DescribeCluster"
    effect    = "Allow"
    actions   = ["eks:DescribeCluster"]
    resources = [aws_eks_cluster.main.arn]
  }
}

resource "aws_iam_role_policy" "cicd" {
  count = var.github_repository == "" ? 0 : 1

  name_prefix = "chronos-cicd-"
  role        = aws_iam_role.cicd[0].id
  policy      = data.aws_iam_policy_document.cicd[0].json
}

# In-cluster authorization for the CI/CD role, scoped to the Chronos namespace.
# The pipeline can roll out Chronos and nothing else.
resource "aws_eks_access_entry" "cicd" {
  count = var.github_repository == "" ? 0 : 1

  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_iam_role.cicd[0].arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "cicd" {
  count = var.github_repository == "" ? 0 : 1

  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_iam_role.cicd[0].arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"

  access_scope {
    type       = "namespace"
    namespaces = [var.kubernetes_namespace]
  }

  depends_on = [aws_eks_access_entry.cicd]
}

# ---------------------------------------------------------------------------
# Secrets Store CSI driver
# ---------------------------------------------------------------------------

# The driver mounts secrets as files in the pod using the pod's own IRSA identity,
# so there is no controller holding broad read access to Secrets Manager and no
# Kubernetes Secret object for a cluster-wide reader to list.
resource "aws_eks_addon" "secrets_store_csi" {
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = "aws-secrets-store-csi-driver"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "PRESERVE"

  depends_on = [aws_eks_node_group.main]
}
