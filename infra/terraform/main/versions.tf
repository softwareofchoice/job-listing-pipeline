terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State lives in the bucket made by ../bootstrap (ADR-033). The bucket name
  # and region come from backend.hcl (git-ignored; see backend.hcl.example),
  # because a backend block can't use variables:
  #   terraform init -backend-config=backend.hcl
  backend "s3" {
    key          = "jobpipe/main.tfstate"
    encrypt      = true
    use_lockfile = true # a .tflock object in the bucket: no DynamoDB table needed
  }
}
