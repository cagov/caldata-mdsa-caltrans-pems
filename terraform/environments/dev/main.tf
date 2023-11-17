##################################
#        Terraform Setup         #
##################################

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.56.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.71"
    }
  }

  backend "s3" {
  }
}

locals {
  owner       = "caltrans"
  environment = "dev"
  project     = "pems"
  region      = "us-west-2"
  locator     = "NGB13288"
}

provider "aws" {
  region = local.region

  default_tags {
    tags = {
      Owner       = local.owner
      Project     = local.project
      Environment = local.environment
    }
  }
}

# This provider is intentionally low-permission. In Snowflake, object creators are
# the default owners of the object. To control the owner, we create different provider
# blocks with different roles, and require that all snowflake resources explicitly
# flag the role they want for the creator.
provider "snowflake" {
  account = local.locator
  role    = "PUBLIC"
}

# Snowflake provider for account administration (to be used only when necessary).
provider "snowflake" {
  alias   = "accountadmin"
  account = local.locator
  role    = "ACCOUNTADMIN"
}

# Snowflake provider for creating databases, warehouses, etc.
provider "snowflake" {
  alias   = "sysadmin"
  account = local.locator
  role    = "SYSADMIN"
}

# Snowflake provider for managing grants to roles.
provider "snowflake" {
  alias   = "securityadmin"
  account = local.locator
  role    = "SECURITYADMIN"
}

# Snowflake provider for managing user accounts and roles.
provider "snowflake" {
  alias   = "useradmin"
  account = local.locator
  role    = "USERADMIN"
}

############################
#    AWS Infrastructure    #
############################

module "s3_lake" {
  source = "../../modules/s3-lake"
  providers = {
    aws = aws
  }

  prefix = "${local.owner}-${local.project}-${local.environment}"
  region = local.region
}

data "aws_iam_role" "mwaa_execution_role" {
  name = "dse-infra-dev-us-west-2-mwaa-execution-role"
}

resource "aws_iam_role_policy_attachment" "mwaa_execution_role" {
  role       = data.aws_iam_role.mwaa_execution_role.name
  policy_arn = module.s3_lake.pems_raw_read_write_policy.arn
}

############################
# Snowflake Infrastructure #
############################

# Main ELT architecture
module "elt" {
  source = "github.com/cagov/data-infrastructure.git//terraform/snowflake/modules/elt?ref=74a522f"
  providers = {
    snowflake.securityadmin = snowflake.securityadmin,
    snowflake.sysadmin      = snowflake.sysadmin,
    snowflake.useradmin     = snowflake.useradmin,
  }

  environment = upper(local.environment)
}

# Schema for raw PeMS data
resource "snowflake_schema" "pems_raw" {
  provider            = snowflake.sysadmin
  database            = "RAW_${upper(local.environment)}"
  name                = "CLEARINGHOUSE"
  data_retention_days = 14
}

# External stage
resource "snowflake_storage_integration" "pems_raw" {
  provider                  = snowflake.accountadmin
  name                      = "PEMS_RAW_${upper(local.environment)}"
  type                      = "EXTERNAL_STAGE"
  storage_provider          = "S3"
  storage_aws_role_arn      = module.s3_lake.snowflake_storage_integration_role.arn
  storage_allowed_locations = ["s3://${module.s3_lake.pems_raw_bucket.name}"]
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
  name                = "PEMS_RAW_${upper(local.environment)}"
  url                 = "s3://${module.s3_lake.pems_raw_bucket.name}"
  database            = snowflake_schema.pems_raw.database
  schema              = snowflake_schema.pems_raw.name
  storage_integration = snowflake_storage_integration.pems_raw.name
}

resource "snowflake_stage_grant" "pems_raw" {
  provider               = snowflake.sysadmin
  database_name          = snowflake_stage.pems_raw.database
  schema_name            = snowflake_stage.pems_raw.schema
  roles                  = ["LOADER_${upper(local.environment)}"]
  privilege              = "USAGE"
  stage_name             = snowflake_stage.pems_raw.name
  enable_multiple_grants = true
}

output "pems_raw_stage" {
  value = {
    storage_aws_external_id  = snowflake_storage_integration.pems_raw.storage_aws_external_id
    storage_aws_iam_user_arn = snowflake_storage_integration.pems_raw.storage_aws_iam_user_arn
  }
}
