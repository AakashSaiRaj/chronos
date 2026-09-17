# Backend config for the dev environment.
#
#   terraform init -backend-config=envs/dev.backend.hcl
#
# The bucket and lock table are per-account bootstrap resources created once,
# outside this module -- a module cannot store its own state. Substitute the real
# account-specific names before use.

bucket       = "chronos-terraform-state-CHANGEME"
key          = "chronos/dev/terraform.tfstate"
region       = "us-east-1"
encrypt      = true
use_lockfile = true
