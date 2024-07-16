######################################
#            Terraform               #
######################################

terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.89"
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

# Schema for raw clearninghouse data
resource "snowflake_schema" "pems_clearinghouse" {
  provider            = snowflake.sysadmin
  database            = "RAW_${var.environment}"
  name                = "CLEARINGHOUSE"
  data_retention_days = 14
}

# Schema for raw data relay server data
resource "snowflake_schema" "pems_db96" {
  provider            = snowflake.sysadmin
  database            = "RAW_${var.environment}"
  name                = "DB96"
  data_retention_days = 14
}

# Storage integration
resource "snowflake_storage_integration" "pems_storage_integration" {
  provider                  = snowflake.accountadmin
  name                      = "PEMS_${var.environment}"
  type                      = "EXTERNAL_STAGE"
  storage_provider          = "S3"
  storage_aws_role_arn      = var.storage_aws_role_arn
  storage_allowed_locations = [var.raw_s3_url, var.marts_s3_url]
}

resource "snowflake_integration_grant" "pems_storage_integration_to_sysadmin" {
  provider               = snowflake.accountadmin
  integration_name       = snowflake_storage_integration.pems_storage_integration.name
  privilege              = "USAGE"
  roles                  = ["SYSADMIN"]
  enable_multiple_grants = true
}

# Raw stage
resource "snowflake_stage" "pems_raw" {
  provider            = snowflake.sysadmin
  name                = "PEMS_RAW_${var.environment}"
  url                 = var.raw_s3_url
  database            = snowflake_schema.pems_clearinghouse.database
  schema              = snowflake_schema.pems_clearinghouse.name
  storage_integration = snowflake_storage_integration.pems_storage_integration.name
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

# Marts stage


resource "snowflake_stage" "pems_marts" {
  provider            = snowflake.sysadmin
  name                = "PEMS_MARTS_${var.environment}"
  url                 = var.marts_s3_url
  database            = "ANALYTICS_${var.environment}"
  schema              = "PUBLIC"
  storage_integration = snowflake_storage_integration.pems_storage_integration.name
}

resource "snowflake_stage_grant" "pems_marts" {
  provider      = snowflake.sysadmin
  database_name = snowflake_stage.pems_marts.database
  schema_name   = snowflake_stage.pems_marts.schema
  roles         = ["TRANSFORMER_${var.environment}"]
  privilege     = "USAGE"
  stage_name    = snowflake_stage.pems_marts.name
}

# Pipes
resource "snowflake_pipe" "station_raw_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_clearinghouse.database
  schema      = snowflake_schema.pems_clearinghouse.name
  name        = "STATION_RAW"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/raw_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_clearinghouse.database
      schema      = snowflake_schema.pems_clearinghouse.name
      table       = "STATION_RAW"
      stage       = snowflake_stage.pems_raw.name
      file_format = "STATION_RAW"
    },
  )
}

resource "snowflake_pipe" "station_meta_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_clearinghouse.database
  schema      = snowflake_schema.pems_clearinghouse.name
  name        = "STATION_META"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/meta_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_clearinghouse.database
      schema      = snowflake_schema.pems_clearinghouse.name
      table       = "STATION_META"
      stage       = snowflake_stage.pems_raw.name
      file_format = "STATION_META"
    },
  )
}

resource "snowflake_pipe" "station_status_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_clearinghouse.database
  schema      = snowflake_schema.pems_clearinghouse.name
  name        = "STATION_STATUS"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/status_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_clearinghouse.database
      schema      = snowflake_schema.pems_clearinghouse.name
      table       = "STATION_STATUS"
      stage       = snowflake_stage.pems_raw.name
      file_format = "STATION_STATUS"
    },
  )
}

resource "snowflake_pipe" "vds30sec_pipe" {
  provider    = snowflake.sysadmin
  database    = snowflake_schema.pems_db96.database
  schema      = snowflake_schema.pems_db96.name
  name        = "VDS30SEC"
  auto_ingest = true

  copy_statement = templatefile(
    "${path.module}/vds30sec_pipe.sql.tplfile",
    {
      database    = snowflake_schema.pems_db96.database
      schema      = snowflake_schema.pems_db96.name
      table       = "VDS30SEC"
      stage       = snowflake_stage.pems_raw.name
      file_format = "VDS30SEC"
    },
  )
}
