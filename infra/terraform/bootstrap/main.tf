# Bootstrap: the S3 bucket that holds Terraform's state for ../main (ADR-033).
#
# Chicken and egg: ../main keeps its state IN this bucket, so the bucket can't
# be created by ../main. This tiny config creates it once and keeps its own
# state in a LOCAL file (terraform.tfstate here, git-ignored). If that file is
# ever lost, nothing breaks: `terraform import` the bucket back in.
#
#   terraform init && terraform apply
#   terraform output -raw backend_hcl > ../main/backend.hcl

terraform {
  required_version = ">= 1.10" # S3-native state locking (use_lockfile) in ../main

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

variable "region" {
  description = "AWS region for the state bucket"
  type        = string
  default     = "us-east-2"
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project    = "jobpipe"
      managed_by = "terraform"
      stack      = "bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}

# Bucket names are global across ALL AWS accounts; the account id makes ours unique.
resource "aws_s3_bucket" "tfstate" {
  bucket = "jobpipe-tfstate-${data.aws_caller_identity.current.account_id}"

  # Deleting this would orphan every resource ../main manages.
  lifecycle {
    prevent_destroy = true
  }
}

# Every state write becomes a new version: a bad apply can be rolled back.
resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # SSE-S3: AWS-managed keys, no cost
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
  depends_on = [aws_s3_bucket_versioning.tfstate]
}

output "state_bucket" {
  value = aws_s3_bucket.tfstate.bucket
}

# Paste-ready backend settings for ../main (terraform init -backend-config=backend.hcl).
output "backend_hcl" {
  value = <<-EOT
    bucket = "${aws_s3_bucket.tfstate.bucket}"
    region = "${var.region}"
  EOT
}
