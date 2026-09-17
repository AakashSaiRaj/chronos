# RDS PostgreSQL: the single source of truth for every workflow, task, and
# history event in Chronos, and also its task queue.
#
# That second role is why the tuning below is not boilerplate. Claiming a task is
# an UPDATE ... FOR UPDATE SKIP LOCKED, so queue throughput is bounded by commit
# latency, and connection count is bounded by the number of server and worker
# replicas rather than by request volume.

resource "aws_db_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.database[*].id

  tags = { Name = local.name }
}

resource "aws_security_group" "database" {
  name_prefix = "${local.name}-db-"
  description = "Chronos PostgreSQL"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-db" }

  lifecycle {
    create_before_destroy = true
  }
}

# Only the cluster nodes may connect, and only on the PostgreSQL port. There is
# deliberately no egress rule: the database has no reason to originate traffic.
resource "aws_vpc_security_group_ingress_rule" "database_from_nodes" {
  security_group_id            = aws_security_group.database.id
  description                  = "PostgreSQL from cluster nodes"
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.nodes.id
}

resource "aws_kms_key" "database" {
  description             = "Chronos ${var.environment} RDS encryption at rest"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "database" {
  name          = "alias/${local.name}-rds"
  target_key_id = aws_kms_key.database.key_id
}

resource "random_password" "database" {
  length  = 32
  special = false # avoids escaping problems in DSNs and shell interpolation
}

resource "aws_db_parameter_group" "main" {
  name_prefix = "${local.name}-"
  family      = "postgres17"

  # Sized for the replica count, not the request rate: every server and worker
  # replica holds a pool, so a scale-up event can exhaust connections long before
  # it exhausts CPU.
  parameter {
    name         = "max_connections"
    value        = tostring(var.db_max_connections)
    apply_method = "pending-reboot"
  }

  # Log anything slower than half a second. The queue's claim query is the one to
  # watch: if it starts appearing here, the tasks table needs attention.
  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }

  # A task row is updated on claim, on renewal, and on report, so the tasks table
  # churns far more than a typical OLTP table. Autovacuum at stock settings falls
  # behind and the claim index bloats; these make it more aggressive.
  parameter {
    name  = "autovacuum_vacuum_scale_factor"
    value = "0.05"
  }

  parameter {
    name  = "autovacuum_analyze_scale_factor"
    value = "0.02"
  }

  parameter {
    name  = "log_lock_waits"
    value = "1"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_instance" "main" {
  identifier     = local.name
  engine         = "postgres"
  engine_version = "17"
  instance_class = var.db_instance_class

  allocated_storage     = var.db_allocated_storage_gb
  max_allocated_storage = var.db_max_allocated_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.database.arn

  db_name  = "chronos"
  username = "chronos"
  password = random_password.database.result
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.database.id]
  publicly_accessible    = false

  parameter_group_name = aws_db_parameter_group.main.name

  # Chronos keeps no authoritative state anywhere else, so a single-AZ database is
  # a single point of failure for the entire system.
  multi_az = var.db_multi_az

  backup_retention_period = var.db_backup_retention_days
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:30-sun:05:30"
  copy_tags_to_snapshot   = true

  auto_minor_version_upgrade = true
  deletion_protection        = var.db_deletion_protection
  skip_final_snapshot        = false
  final_snapshot_identifier  = "${local.name}-final"

  performance_insights_enabled          = true
  performance_insights_retention_period = 7
  enabled_cloudwatch_logs_exports       = ["postgresql", "upgrade"]

  # Terraform must not be the thing that changes a database password. Rotation
  # goes through Secrets Manager, which updates the secret and the instance
  # together; without this, the next plan would try to revert it.
  lifecycle {
    ignore_changes = [password]
  }
}

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

# The DSN lives in Secrets Manager and is read at pod start through the Secrets
# Store CSI driver, so it is never baked into an image, a manifest, or a
# Kubernetes Secret that a cluster-wide reader could list.
resource "aws_secretsmanager_secret" "database" {
  name_prefix = "${local.name}/database-"
  description = "Chronos ${var.environment} PostgreSQL connection details"
  kms_key_id  = aws_kms_key.database.arn

  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "database" {
  secret_id = aws_secretsmanager_secret.database.id

  secret_string = jsonencode({
    username = aws_db_instance.main.username
    password = random_password.database.result
    host     = aws_db_instance.main.address
    port     = aws_db_instance.main.port
    dbname   = aws_db_instance.main.db_name

    # sslmode=require, not disable: the traffic stays inside the VPC, but "inside
    # the VPC" is not a trust boundary worth betting the credential store on.
    url = format(
      "postgres://%s:%s@%s:%d/%s?sslmode=require",
      aws_db_instance.main.username,
      random_password.database.result,
      aws_db_instance.main.address,
      aws_db_instance.main.port,
      aws_db_instance.main.db_name,
    )
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
