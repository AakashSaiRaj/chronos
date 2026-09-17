// Production environment.
//
// Chronos holds all authoritative workflow state in PostgreSQL and uses it as its
// task queue, so the database is both the single point of failure and the
// throughput ceiling. That is why Multi-AZ, real backup retention, and deletion
// protection are non-negotiable here, and why the instance class is the number to
// revisit after Phase 4 load testing rather than guess at now.

environment = "prod"
region      = "us-east-1"

// Three AZs and a NAT gateway per AZ: losing one AZ must not cut egress for the
// surviving two.
availability_zone_count = 3
single_nat_gateway      = false

node_instance_types     = ["m6i.large"]
node_group_min_size     = 3
node_group_max_size     = 12
node_group_desired_size = 3

db_instance_class           = "db.r6g.large"
db_allocated_storage_gb     = 100
db_max_allocated_storage_gb = 500

db_multi_az              = true
db_backup_retention_days = 30
db_deletion_protection   = true

// Must cover every server and worker replica's pool at maximum HPA scale, with
// headroom. Exhausting connections during a scale-up event would turn a traffic
// spike into an outage.
db_max_connections = 500

// Both off. See the variable descriptions: Chronos does not read from a cache,
// and its queue is the tasks table by design. Turning these on before something
// uses them buys a bill and two more things that can be down.
enable_redis = false
enable_sqs   = false

// The API endpoint stays private. Access is via a bastion or VPN; add an office
// CIDR here only with a deliberate decision behind it.
cluster_endpoint_public_access_cidrs = []
