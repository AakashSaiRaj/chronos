terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # State lives in S3 with DynamoDB locking. Left partial on purpose: the bucket
  # and table are per-account bootstrap resources, so they are supplied at init
  # time (see envs/*.backend.hcl) rather than hardcoded into the module.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "chronos"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
