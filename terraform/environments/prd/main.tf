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
      version = "0.69"
    }
  }

  backend "s3" {
  }
}

locals {
  owner       = "caltrans"
  environment = "prd"
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

############################
# Snowflake Infrastructure #
############################

module "elt" {
  source = "github.com/cagov/data-infrastructure.git//terraform/snowflake/modules/elt?ref=74a522f"
  providers = {
    snowflake.securityadmin = snowflake.securityadmin,
    snowflake.sysadmin      = snowflake.sysadmin,
    snowflake.useradmin     = snowflake.useradmin,
  }

  environment = upper(local.environment)
}
