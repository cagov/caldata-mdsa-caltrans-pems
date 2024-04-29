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
      version = "0.89"
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

  # These are circular dependencies on the outputs. Unfortunate, but
  # necessary, as we don't know them until we've created the storage
  # integration, which itself depends on the assume role policy.
  storage_aws_external_id  = "NGB13288_SFCRole=2_P94CCaZYR9XFUzpMIGN6HOit/zQ="
  storage_aws_iam_user_arn = "arn:aws:iam::946158320428:user/uunc0000-s"
  pipe_sqs_queue_arn       = "arn:aws:sqs:us-west-2:946158320428:sf-snowpipe-AIDA5YS3OHMWCVTR5XHEE-YZjsweK3loK4rXlOJBWF_g"
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

  prefix                                         = "${local.owner}-${local.project}-${local.environment}"
  region                                         = local.region
  snowflake_raw_storage_integration_iam_user_arn = local.storage_aws_iam_user_arn
  snowflake_raw_storage_integration_external_id  = local.storage_aws_external_id
  snowflake_pipe_sqs_queue_arn                   = local.pipe_sqs_queue_arn
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
  source = "github.com/cagov/data-infrastructure.git//terraform/snowflake/modules/elt?ref=5fe13b2"
  providers = {
    snowflake.accountadmin  = snowflake.accountadmin,
    snowflake.securityadmin = snowflake.securityadmin,
    snowflake.sysadmin      = snowflake.sysadmin,
    snowflake.useradmin     = snowflake.useradmin,
  }

  environment = upper(local.environment)
}

module "snowflake_clearinghouse" {
  source = "../../modules/snowflake-clearinghouse"
  providers = {
    snowflake.accountadmin  = snowflake.accountadmin,
    snowflake.securityadmin = snowflake.securityadmin,
    snowflake.sysadmin      = snowflake.sysadmin,
    snowflake.useradmin     = snowflake.useradmin,
  }

  environment          = upper(local.environment)
  s3_url               = "s3://${module.s3_lake.pems_raw_bucket.name}"
  storage_aws_role_arn = module.s3_lake.snowflake_storage_integration_role.arn
}

output "pems_raw_stage" {
  value = module.snowflake_clearinghouse.pems_raw_stage
}

output "notification_channel" {
  value = module.snowflake_clearinghouse.notification_channel
}
