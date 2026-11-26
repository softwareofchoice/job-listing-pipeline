{#
  S3 access for DuckDB, only when the raw layer is read from S3 (ADR-031).

  Runs as an on-run-start hook. With the default raw_root (data/raw) it emits
  nothing, so local builds never load the S3 extensions or need a network.

  With JOBPIPE_RAW_ROOT=s3://<bucket>/raw it creates a DuckDB secret using
  the AWS "credential chain" limited to the environment: DuckDB reads
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY itself (the pipeline user's key,
  ADR-032). The key never appears in this SQL, so it can't leak into dbt's
  logs or target/ files. Not 'config' (~/.aws): if that link of the chain
  fails, e.g. no [default] profile, DuckDB rejects the whole secret.

  JOBPIPE_S3_ENDPOINT (optional) points at an S3-compatible server instead of
  AWS, e.g. 127.0.0.1:5055 for a local moto or MinIO in tests.
#}
{% macro s3_secret() %}
  {%- if var('raw_root').startswith('s3://') -%}
    {%- set endpoint = env_var('JOBPIPE_S3_ENDPOINT', '') -%}
    create or replace secret jobpipe_s3 (
        type s3,
        provider credential_chain,
        chain 'env',
        region '{{ env_var("AWS_REGION", "us-east-2") }}'
        {%- if endpoint %},
        endpoint '{{ endpoint }}',
        url_style 'path',
        use_ssl false
        {%- endif %}
    )
  {%- endif -%}
{% endmacro %}
