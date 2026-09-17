// Development environment: sized down, guardrails relaxed where a mistake is
// cheap. Anything that would hide a production problem is left at production
// settings.

environment = "dev"
region      = "us-east-1"

// Two AZs, one NAT. Halves the NAT bill; a single AZ failure cuts egress, which
// is an acceptable trade in dev and is not in prod.
availability_zone_count = 2
single_nat_gateway      = true

node_instance_types     = ["t3.medium"]
node_group_min_size     = 2
node_group_max_size     = 4
node_group_desired_size = 2

db_instance_class           = "db.t4g.micro"
db_allocated_storage_gb     = 20
db_max_allocated_storage_gb = 50

// Single-AZ and destroyable: dev data is expendable, and being able to tear the
// environment down cheaply is the point of having one.
db_multi_az              = false
db_backup_retention_days = 1
db_deletion_protection   = false

// Still sized for the replica count rather than the instance size. A connection
// exhaustion bug must reproduce in dev, not first appear in prod.
db_max_connections = 200

enable_redis = false
enable_sqs   = false
