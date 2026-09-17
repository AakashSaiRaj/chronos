# Backend config for the prod environment.
#
#   terraform init -backend-config=envs/prod.backend.hcl
#
# Separate state file from dev, in the same bucket. Separate state is what makes
# "terraform destroy" in dev incapable of touching production.

bucket       = "chronos-terraform-state-CHANGEME"
key          = "chronos/prod/terraform.tfstate"
region       = "us-east-1"
encrypt      = true
use_lockfile = true
