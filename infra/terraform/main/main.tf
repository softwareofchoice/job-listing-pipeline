# The data lake bucket and the pipeline's own IAM user (ADR-030, ADR-032,
# ADR-037). Glue tables: glue.tf. Athena workgroup: athena.tf.

variable "region" {
  description = "AWS region for the data bucket"
  type        = string
  default     = "us-east-2" # Ohio: closest to Chicago
}

variable "noncurrent_version_days" {
  description = "Days to keep overwritten or deleted object versions"
  type        = number
  default     = 30
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project    = "jobpipe"
      managed_by = "terraform"
      stack      = "main"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  raw_prefix = "raw"
  # Prefixes the pipeline user may write: raw responses and curated Parquet.
  pipeline_prefixes = [local.raw_prefix, local.curated_prefix]
}

# ---------------------------------------------------------------- bucket ----

resource "aws_s3_bucket" "data" {
  # Globally unique thanks to the account id.
  bucket = "jobpipe-data-${data.aws_caller_identity.current.account_id}-${var.region}"
}

# Disables ACLs: only IAM and bucket policies decide access.
resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-S3. Also what makes jobpipe.upload's MD5 comparison valid: with SSE-S3
# (not SSE-KMS) a single-part object's ETag is the MD5 of its bytes.
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Re-ingesting a day overwrites its files (ADR-002); versioning keeps the
# previous copy for a while, so an overwrite or delete can be undone.
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
  }

  # Athena writes every query's result as a CSV here; they're only needed
  # until you've read them.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # No transition to cheaper storage classes: our objects are a few KB, and
  # Standard-IA bills every object as at least 128 KB.

  depends_on = [aws_s3_bucket_versioning.data]
}

# Refuse plain-HTTP requests, even correctly signed ones.
data "aws_iam_policy_document" "data_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.data.arn,
      "${aws_s3_bucket.data.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = data.aws_iam_policy_document.data_bucket.json
  # The public access block must exist first, or AWS may reject the policy.
  depends_on = [aws_s3_bucket_public_access_block.data]
}

# ------------------------------------------------------ pipeline identity ----

# Used only by the scheduled pipeline (Airflow). Its access key is created by
# hand with the AWS CLI, NOT here: a key made by Terraform is stored in plain
# text in the state file (ADR-032).
resource "aws_iam_user" "pipeline" {
  name = "jobpipe-pipeline"
  path = "/jobpipe/"
}

# Least privilege: list, read and write under raw/ and curated/ in this
# bucket (ADR-032, extended by ADR-037). Nothing else: no delete, no other
# prefixes or buckets, no Glue or Athena, no IAM.
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListPipelinePrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = flatten([for p in local.pipeline_prefixes : ["${p}/", "${p}/*"]])
    }
  }

  statement {
    sid       = "ReadWritePipelineObjects"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = [for p in local.pipeline_prefixes : "${aws_s3_bucket.data.arn}/${p}/*"]
  }
}

resource "aws_iam_policy" "pipeline" {
  # Name and description kept from Stage 5b although it now covers curated/
  # too: changing either forces a replace (detach, delete, recreate). Only the
  # policy document below can change in place.
  name        = "jobpipe-pipeline-raw-rw"
  path        = "/jobpipe/"
  description = "jobpipe pipeline: list/read/write raw/ in the data bucket, no delete"
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# ---------------------------------------------------------------- outputs ----

output "data_bucket" {
  description = "Put in .env as JOBPIPE_S3_BUCKET"
  value       = aws_s3_bucket.data.bucket
}

output "region" {
  description = "Put in .env as AWS_REGION"
  value       = var.region
}

output "pipeline_user" {
  description = "Create its key with: aws iam create-access-key --user-name <this>"
  value       = aws_iam_user.pipeline.name
}
