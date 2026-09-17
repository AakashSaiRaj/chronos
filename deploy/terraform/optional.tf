# Optional infrastructure: ECR (always), plus Redis and SQS behind feature flags.
#
# Redis and SQS are off by default. The reasoning is in their variable
# descriptions and is worth restating, because provisioning them "because the
# architecture diagram has them" is how systems acquire dependencies nobody can
# explain: Chronos's queue is the tasks table by design, and nothing in the
# current codebase reads from a cache. Unused infrastructure is not free -- it is
# a bill, an attack surface, and another thing that can be down.

# ---------------------------------------------------------------------------
# Container registry
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "chronos" {
  name                 = local.name
  image_tag_mutability = "IMMUTABLE" # a deployed tag can never be repointed

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
  }
}

resource "aws_ecr_lifecycle_policy" "chronos" {
  repository = aws_ecr_repository.chronos.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images quickly; they are build intermediates."
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep the most recent tagged images so a rollback target always exists."
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = var.image_retention_count
        }
        action = { type = "expire" }
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# Redis (disabled by default)
# ---------------------------------------------------------------------------

resource "aws_elasticache_subnet_group" "main" {
  count = var.enable_redis ? 1 : 0

  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_security_group" "redis" {
  count = var.enable_redis ? 1 : 0

  name_prefix = "${local.name}-redis-"
  description = "Chronos ElastiCache Redis"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-redis" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "redis_from_nodes" {
  count = var.enable_redis ? 1 : 0

  security_group_id            = aws_security_group.redis[0].id
  description                  = "Redis from cluster nodes"
  from_port                    = 6379
  to_port                      = 6379
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.nodes.id
}

resource "aws_elasticache_replication_group" "main" {
  count = var.enable_redis ? 1 : 0

  replication_group_id = local.name
  description          = "Chronos ${var.environment} cache"

  engine         = "redis"
  engine_version = "7.1"
  node_type      = var.redis_node_type
  port           = 6379

  # Two nodes with automatic failover: a single-node cache that disappears takes
  # whatever depends on it with it.
  num_cache_clusters         = 2
  automatic_failover_enabled = true

  subnet_group_name  = aws_elasticache_subnet_group.main[0].name
  security_group_ids = [aws_security_group.redis[0].id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true

  maintenance_window       = "sun:05:30-sun:06:30"
  snapshot_retention_limit = 3
}

# ---------------------------------------------------------------------------
# SQS (disabled by default)
# ---------------------------------------------------------------------------

# Note the shape: a queue plus a real dead-letter queue with a redrive policy.
# Chronos already implements dead-lettering for tasks in PostgreSQL; if SQS is
# ever used for notifications, its own poison messages need the same treatment,
# and a queue without a DLQ silently drops them.
resource "aws_sqs_queue" "dead_letter" {
  count = var.enable_sqs ? 1 : 0

  name                      = "${local.name}-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "main" {
  count = var.enable_sqs ? 1 : 0

  name                       = local.name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600 # 4 days
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter[0].arn
    maxReceiveCount     = 5
  })
}
