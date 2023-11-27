######################################
#            Terraform               #
######################################

terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.71"
      configuration_aliases = [
        snowflake.accountadmin,
        snowflake.securityadmin,
        snowflake.sysadmin,
        snowflake.useradmin,
      ]
    }
  }
  required_version = ">= 1.0"
}

# Schema for raw PeMS data
resource "snowflake_schema" "pems_raw" {
  provider            = snowflake.sysadmin
  database            = "RAW_${var.environment}"
  name                = "CLEARINGHOUSE"
  data_retention_days = 14
}

# External stage
resource "snowflake_storage_integration" "pems_raw" {
  provider                  = snowflake.accountadmin
  name                      = "PEMS_RAW_${var.environment}"
  type                      = "EXTERNAL_STAGE"
  storage_provider          = "S3"
  storage_aws_role_arn      = var.storage_aws_role_arn
  storage_allowed_locations = [var.s3_url]
}

resource "snowflake_integration_grant" "pems_raw_to_sysadmin" {
  provider               = snowflake.accountadmin
  integration_name       = snowflake_storage_integration.pems_raw.name
  privilege              = "USAGE"
  roles                  = ["SYSADMIN"]
  enable_multiple_grants = true
}


resource "snowflake_stage" "pems_raw" {
  provider            = snowflake.sysadmin
  name                = "PEMS_RAW_${var.environment}"
  url                 = var.s3_url
  database            = snowflake_schema.pems_raw.database
  schema              = snowflake_schema.pems_raw.name
  storage_integration = snowflake_storage_integration.pems_raw.name
}

resource "snowflake_stage_grant" "pems_raw" {
  provider               = snowflake.sysadmin
  database_name          = snowflake_stage.pems_raw.database
  schema_name            = snowflake_stage.pems_raw.schema
  roles                  = ["LOADER_${var.environment}"]
  privilege              = "USAGE"
  stage_name             = snowflake_stage.pems_raw.name
  enable_multiple_grants = true
}

# Pipes
resource "snowflake_pipe" "station_raw_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_raw.database
  schema      = snowflake_schema.pems_raw.name
  name        = "STATION_RAW"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/raw_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_raw.database
      schema      = snowflake_schema.pems_raw.name
      table       = "STATION_RAW"
      stage       = snowflake_stage.pems_raw.name
      file_format = "STATION_RAW"
    },
  )
}

resource "snowflake_pipe" "station_meta_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_raw.database
  schema      = snowflake_schema.pems_raw.name
  name        = "STATION_META"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/meta_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_raw.database
      schema      = snowflake_schema.pems_raw.name
      table       = "STATION_META"
      stage       = snowflake_stage.pems_raw.name
      file_format = "STATION_META"
    },
  )
}

resource "snowflake_pipe" "station_status_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_raw.database
  schema      = snowflake_schema.pems_raw.name
  name        = "STATION_STATUS"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/status_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_raw.database
      schema      = snowflake_schema.pems_raw.name
      table       = "STATION_STATUS"
      stage       = snowflake_stage.pems_raw.name
      file_format = "STATION_STATUS"
    },
  )
}
