# Outputs are what the Kustomize overlays and the CI/CD pipeline consume, so the
# handoff from Terraform to Kubernetes is explicit rather than copy-pasted.
#
# Nothing here contains a credential. The database DSN lives in Secrets Manager
# and only its ARN is exported, so `terraform output` cannot leak it and neither
# can the state file's plaintext outputs.

output "cluster_name" {
  description = "EKS cluster name; used by `aws eks update-kubeconfig`."
  value       = aws_eks_cluster.main.name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = aws_eks_cluster.main.endpoint
}

output "cluster_oidc_issuer" {
  description = "OIDC issuer URL backing IRSA."
  value       = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

output "region" {
  description = "Deployment region."
  value       = var.region
}

output "namespace" {
  description = "Kubernetes namespace for Chronos workloads."
  value       = var.kubernetes_namespace
}

output "ecr_repository_url" {
  description = "Image repository. The CD pipeline pushes here."
  value       = aws_ecr_repository.chronos.repository_url
}

output "database_endpoint" {
  description = "RDS endpoint hostname."
  value       = aws_db_instance.main.address
}

output "database_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the DSN. Referenced by the SecretProviderClass, never by a manifest containing the value."
  value       = aws_secretsmanager_secret.database.arn
}

output "database_secret_name" {
  description = "Name of the database secret, for the SecretProviderClass objectName."
  value       = aws_secretsmanager_secret.database.name
}

output "server_role_arn" {
  description = "IRSA role for the API and scheduler service account."
  value       = aws_iam_role.server.arn
}

output "worker_role_arn" {
  description = "IRSA role for the worker service account."
  value       = aws_iam_role.worker.arn
}

output "cicd_role_arn" {
  description = "Role GitHub Actions assumes via OIDC. Empty when github_repository is unset."
  value       = var.github_repository == "" ? "" : aws_iam_role.cicd[0].arn
}

output "redis_endpoint" {
  description = "Redis primary endpoint, empty unless enable_redis is set."
  value       = var.enable_redis ? aws_elasticache_replication_group.main[0].primary_endpoint_address : ""
}

output "sqs_queue_url" {
  description = "SQS queue URL, empty unless enable_sqs is set."
  value       = var.enable_sqs ? aws_sqs_queue.main[0].url : ""
}

# A single blob the deploy pipeline can consume in one step, so the k8s overlay
# does not need to know the name of every individual output.
output "kustomize_values" {
  description = "Values the Kustomize overlay needs, ready to render into a patch."
  value = {
    namespace           = var.kubernetes_namespace
    image_repository    = aws_ecr_repository.chronos.repository_url
    server_role_arn     = aws_iam_role.server.arn
    worker_role_arn     = aws_iam_role.worker.arn
    database_secret_arn = aws_secretsmanager_secret.database.arn
    region              = var.region
  }
}
