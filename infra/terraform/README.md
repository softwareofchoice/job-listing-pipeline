# Terraform: S3 data lake and pipeline user (Stage 5b)

Two stacks, applied in order:

| Stack | Creates | State kept in |
|---|---|---|
| `bootstrap/` | `jobpipe-tfstate-<account>`: versioned, encrypted bucket for Terraform state | local `bootstrap/terraform.tfstate` (git-ignored) |
| `main/` | `jobpipe-data-<account>-<region>` data bucket; `jobpipe-pipeline` IAM user with access to `raw/` and `curated/` only; Glue database `jobpipe` with the curated tables; Athena workgroup `jobpipe` | the bootstrap bucket (`jobpipe/main.tfstate`) |

Why two: `main` stores its state in a bucket, and that bucket has to exist
before `main` can run (ADR-033). Decisions: ADR-030 to ADR-037 in
[docs/decisions.md](../../docs/decisions.md).

Cost: a few MB in S3 Standard plus requests, well under $0.10 a month.

## Prerequisites

- **Terraform >= 1.10** and the **AWS CLI v2**.
- **Your own admin credentials** for running Terraform, e.g. an IAM Identity
  Center (SSO) profile: `aws sso login --profile <you>`, then
  `export AWS_PROFILE=<you>`. Check with `aws sts get-caller-identity`.
  These are *not* the pipeline's credentials.

> Don't run Terraform in a shell where you sourced `.env`: the pipeline user's
> `AWS_ACCESS_KEY_ID` in the environment takes precedence over `AWS_PROFILE`,
> and that user may only touch `raw/`.

## First-time setup

```bash
cd infra/terraform

# 1. State bucket
terraform -chdir=bootstrap init
terraform -chdir=bootstrap apply
terraform -chdir=bootstrap output -raw backend_hcl > main/backend.hcl

# 2. Data bucket + pipeline user
terraform -chdir=main init -backend-config=backend.hcl
terraform -chdir=main plan      # read it: what will be created?
terraform -chdir=main apply

# 3. The pipeline's access key, created OUTSIDE Terraform (ADR-032).
#    The secret is printed once: put it straight into .env.
aws iam create-access-key --user-name "$(terraform -chdir=main output -raw pipeline_user)"
terraform -chdir=main output -raw data_bucket    # -> JOBPIPE_S3_BUCKET in .env
```

Then in the repo root's `.env` (see `.env.example`):

```bash
JOBPIPE_S3_BUCKET=jobpipe-data-<account>-us-east-2
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

Try it: `PYTHONPATH=src .venv/bin/python -m jobpipe.upload --dry-run`.

## Day to day

- A dbt model's columns changed (`tests/test_export.py` fails): run
  `PYTHONPATH=src .venv/bin/python -m jobpipe.export --write-schemas` from the
  repo root, commit `main/curated_schemas.json`, then `plan` and `apply`.

- Change something: edit `main/*.tf`, then `terraform -chdir=main plan` and `apply`.
- Rotate the pipeline key: `aws iam create-access-key` (a user may hold two),
  update `.env`, then `aws iam delete-access-key --access-key-id <old>`.
- Tear down: empty the data bucket (all versions), then `terraform -chdir=main destroy`.
  The state bucket has `prevent_destroy`; remove that line first if you really mean it.

`.terraform.lock.hcl` is committed: it pins the provider version and checksums
so every machine gets the same provider. Update it with `terraform init -upgrade`.
