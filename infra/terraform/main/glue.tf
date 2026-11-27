# Glue Data Catalog: the schemas Athena reads the curated Parquet with
# (ADR-034, ADR-035, ADR-036).
#
# Columns come from curated_schemas.json, generated from the dbt warehouse by
# `python -m jobpipe.export --write-schemas`; tests/test_export.py fails if it
# no longer matches what the export writes.

locals {
  curated_prefix = "curated"
  curated_tables = jsondecode(file("${path.module}/curated_schemas.json"))
  curated_root   = "s3://${aws_s3_bucket.data.bucket}/${local.curated_prefix}"
}

variable "curated_first_dt" {
  description = "Earliest dt partition Athena considers (partition projection range start)"
  type        = string
  default     = "2026-11-01"
}

resource "aws_glue_catalog_database" "jobpipe" {
  name        = "jobpipe"
  description = "jobpipe curated tables: daily Parquet snapshots exported from the dbt warehouse"
}

resource "aws_glue_catalog_table" "curated" {
  for_each = local.curated_tables

  name          = each.key
  database_name = aws_glue_catalog_database.jobpipe.name
  description   = "${each.value.description}. dbt model: ${each.value.dbt_model}. One snapshot per dt."
  table_type    = "EXTERNAL_TABLE" # dropping the table never deletes the S3 data

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"

    # Partition projection (ADR-036): Athena computes the dt partitions from
    # these rules at query time, so nothing has to register a new day.
    # A day with no folder simply has no rows.
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "${var.curated_first_dt},NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    # $${dt} is a literal ${dt} for Athena to fill in, not a Terraform variable.
    "storage.location.template" = "${local.curated_root}/${each.key}/dt=$${dt}/"
  }

  partition_keys {
    name    = "dt"
    type    = "string" # 'YYYY-MM-DD'; compare with dt = '2026-11-26'
    comment = "Scoring as-of date of the export (latest fetch)"
  }

  storage_descriptor {
    location      = "${local.curated_root}/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

output "glue_database" {
  value = aws_glue_catalog_database.jobpipe.name
}
