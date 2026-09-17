variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name; used in resource names and tags."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be lowercase alphanumeric with dashes, 2-16 chars."
  }
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "availability_zone_count" {
  description = "How many AZs to spread subnets across. Two is the minimum EKS accepts; three is the minimum for a Multi-AZ RDS failover to have somewhere to go."
  type        = number
  default     = 3

  validation {
    condition     = var.availability_zone_count >= 2 && var.availability_zone_count <= 4
    error_message = "availability_zone_count must be between 2 and 4."
  }
}

variable "single_nat_gateway" {
  description = "Use one NAT gateway for all private subnets. Cheaper, but a single AZ failure cuts egress for the whole cluster, so production should leave this false."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# EKS
# ---------------------------------------------------------------------------

variable "kubernetes_version" {
  description = "EKS control plane version."
  type        = string
  default     = "1.31"
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["t3.large"]
}

variable "node_group_min_size" {
  description = "Minimum nodes. Must leave room for the worker HPA to scale down to its floor."
  type        = number
  default     = 2
}

variable "node_group_max_size" {
  description = "Maximum nodes. Bounds the blast radius of a runaway HPA."
  type        = number
  default     = 6
}

variable "node_group_desired_size" {
  description = "Initial node count."
  type        = number
  default     = 2
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint. Defaults to nothing: the endpoint is private unless a caller is named explicitly."
  type        = list(string)
  default     = []
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

variable "db_instance_class" {
  description = "RDS instance class. Chronos uses PostgreSQL as its task queue, so claim latency tracks commit latency: this is the single most load-bearing sizing decision in the stack."
  type        = string
  default     = "db.t4g.medium"
}

variable "db_allocated_storage_gb" {
  description = "Initial storage in GiB."
  type        = number
  default     = 50
}

variable "db_max_allocated_storage_gb" {
  description = "Upper bound for storage autoscaling. History and completed tasks accumulate, so leave headroom."
  type        = number
  default     = 200
}

variable "db_multi_az" {
  description = "Run a standby in another AZ. Chronos holds all authoritative state here, so a single-AZ database is a single point of failure for the whole system."
  type        = bool
  default     = true
}

variable "db_backup_retention_days" {
  description = "Automated backup retention."
  type        = number
  default     = 14
}

variable "db_deletion_protection" {
  description = "Refuse to destroy the database. Production must leave this on."
  type        = bool
  default     = true
}

variable "db_max_connections" {
  description = "PostgreSQL max_connections. Must cover every server and worker replica's pool with headroom, or a scale-up event exhausts connections before it exhausts CPU."
  type        = number
  default     = 300
}

# ---------------------------------------------------------------------------
# Optional infrastructure
# ---------------------------------------------------------------------------

variable "enable_redis" {
  description = <<-EOT
    Provision ElastiCache Redis.

    Off by default because Chronos does not currently need it. Workflow
    definitions are immutable and cheap to cache in-process, executions are
    read through the database that owns them, and the engine holds no state to
    share between replicas. Turning this on before something actually reads
    from it would add a dependency, a failure mode, and a bill for nothing.

    The module is here so that the day a real use appears -- an API read cache,
    or distributed rate limiting -- it is one variable rather than a project.
  EOT
  type        = bool
  default     = false
}

variable "enable_sqs" {
  description = <<-EOT
    Provision SQS queues (main plus a dead-letter queue).

    Off by default for the same reason. Chronos's task queue is the tasks
    table, claimed with SELECT ... FOR UPDATE SKIP LOCKED, which is a
    deliberate design decision rather than a gap: scheduling a task is a state
    change the engine is already writing, so a table-backed queue commits the
    enqueue in the same transaction and needs no outbox to stay consistent.

    Provisioned on request for two real cases: an external system that wants to
    be notified when a task is dead-lettered, and a future pluggable queue
    backend if PostgreSQL commit throughput becomes the binding constraint.
    Phase 4 load testing is what should decide that, not preference.
  EOT
  type        = bool
  default     = false
}

variable "redis_node_type" {
  description = "ElastiCache node type, when enabled."
  type        = string
  default     = "cache.t4g.micro"
}

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

variable "kubernetes_namespace" {
  description = "Namespace the Chronos workloads run in. Must match the Kustomize overlay, since the IRSA trust policy is scoped to a specific namespace and service account."
  type        = string
  default     = "chronos"
}

variable "image_retention_count" {
  description = "How many container images ECR keeps before expiring the oldest."
  type        = number
  default     = 30
}

variable "github_repository" {
  description = "owner/repo allowed to assume the CI/CD deploy role via GitHub OIDC. Empty disables the role, so a fork cannot inherit deploy rights."
  type        = string
  default     = ""
}
