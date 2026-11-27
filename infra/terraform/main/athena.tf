# Athena workgroup: where query results go, and a cost guard (ADR-037).

variable "athena_bytes_scanned_cutoff" {
  description = "Cancel any query that would scan more than this many bytes"
  type        = number
  default     = 1073741824 # 1 GB = $0.005 at $5/TB; a full day of curated data is ~3 MB
}

resource "aws_athena_workgroup" "jobpipe" {
  name        = "jobpipe"
  description = "Queries over the jobpipe curated tables"

  # Deleting the workgroup also deletes its saved queries and history.
  force_destroy = true

  configuration {
    # Clients can't override the result location or turn off the cutoff.
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

output "athena_workgroup" {
  value = aws_athena_workgroup.jobpipe.name
}
