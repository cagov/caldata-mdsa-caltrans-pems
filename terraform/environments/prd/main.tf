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
      version = "1.0.1"
    }
  }

  backend "s3" {
  }
}

locals {
  owner             = "caltrans"
  environment       = "prd"
  project           = "pems"
  region            = "us-west-2"
  account_name      = "DSE_CALTRANS_PEMS"
  organization_name = "VSB79059"

  # These are circular dependencies on the outputs. Unfortunate, but
  # necessary, as we don't know them until we've created the storage
  # integration, which itself depends on the assume role policy.
  storage_aws_external_id  = "NGB13288_SFCRole=2_DOvQkLCm41n0x9HYZgzyeB0u7j4="
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

provider "aws" {
  alias   = "caltrans"
  region  = local.region
  profile = "caltrans"

  default_tags {
    tags = {
      "BURequestor" : "zhenyu.zhu@dot.ca.gov"
      "BUName" : "Business Intelligence and Automation"
      "BUCode" : "3345"
      "ProjectCode" : "unknown"
      "ProjectPhase" : "unknown"
      "ReportingCode" : "unknown"
      "DataClassification" : "Public"
      "Environment" : "PD"
      "ApplicationName" : "PeMS"
      "Backup" : "No"
      "BusinessCriticality" : "Low"
    }
  }
}

# This provider is intentionally low-permission. In Snowflake, object creators are
# the default owners of the object. To control the owner, we create different provider
# blocks with different roles, and require that all snowflake resources explicitly
# flag the role they want for the creator.
provider "snowflake" {
  role                     = "PUBLIC"
  account_name             = local.account_name
  organization_name        = local.organization_name
  preview_features_enabled = ["snowflake_storage_integration_resource", "snowflake_stage_resource", "snowflake_pipe_resource"]
}

# Snowflake provider for account administration (to be used only when necessary).
provider "snowflake" {
  alias                    = "accountadmin"
  account_name             = local.account_name
  organization_name        = local.organization_name
  role                     = "ACCOUNTADMIN"
  preview_features_enabled = ["snowflake_storage_integration_resource", "snowflake_stage_resource", "snowflake_pipe_resource"]
}

# Snowflake provider for creating databases, warehouses, etc.
provider "snowflake" {
  alias                    = "sysadmin"
  account_name             = local.account_name
  organization_name        = local.organization_name
  role                     = "SYSADMIN"
  preview_features_enabled = ["snowflake_storage_integration_resource", "snowflake_stage_resource", "snowflake_pipe_resource"]
}

# Snowflake provider for managing grants to roles.
provider "snowflake" {
  alias                    = "securityadmin"
  account_name             = local.account_name
  organization_name        = local.organization_name
  role                     = "SECURITYADMIN"
  preview_features_enabled = ["snowflake_storage_integration_resource", "snowflake_stage_resource", "snowflake_pipe_resource"]
}

# Snowflake provider for managing user accounts and roles.
provider "snowflake" {
  alias                    = "useradmin"
  account_name             = local.account_name
  organization_name        = local.organization_name
  role                     = "USERADMIN"
  preview_features_enabled = ["snowflake_storage_integration_resource", "snowflake_stage_resource", "snowflake_pipe_resource"]
}

############################
#    AWS Infrastructure    #
############################

module "s3_lake" {
  source = "../../modules/s3-lake"
  providers = {
    aws = aws
  }

  prefix                                     = "${local.owner}-${local.project}-${local.environment}"
  region                                     = local.region
  snowflake_storage_integration_iam_user_arn = local.storage_aws_iam_user_arn
  snowflake_storage_integration_external_id  = local.storage_aws_external_id
  snowflake_pipe_sqs_queue_arn               = local.pipe_sqs_queue_arn
}

data "aws_iam_role" "mwaa_execution_role" {
  name = "dse-infra-dev-us-west-2-mwaa-execution-role"
}

resource "aws_iam_role_policy_attachment" "mwaa_execution_role" {
  role       = data.aws_iam_role.mwaa_execution_role.name
  policy_arn = module.s3_lake.pems_raw_read_write_policy.arn
}

# Give MWAA execution role access to Caltrans pems-raw bucket for cross-account data copying
# Create policy in ODI account that grants access to Caltrans bucket
# TODO: remove once data relay setup is complete
data "aws_iam_policy_document" "mwaa_caltrans_s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = [
      module.caltrans_s3_lake.pems_raw_bucket.arn,
      "${module.caltrans_s3_lake.pems_raw_bucket.arn}/*"
    ]
  }
}

# TODO: remove once data relay setup is complete
resource "aws_iam_policy" "mwaa_caltrans_s3_access" {
  name        = "${local.owner}-${local.project}-${local.environment}-mwaa-caltrans-pems-raw-access"
  description = "Allow MWAA to access Caltrans pems_raw bucket"
  policy      = data.aws_iam_policy_document.mwaa_caltrans_s3_access.json
}

# TODO: remove once data relay setup is complete
resource "aws_iam_role_policy_attachment" "mwaa_execution_role_caltrans" {
  role       = data.aws_iam_role.mwaa_execution_role.name
  policy_arn = aws_iam_policy.mwaa_caltrans_s3_access.arn
}

###############################
# Caltrans AWS Infrastructure #
###############################

module "caltrans_s3_lake" {
  source = "../../modules/s3-lake"
  providers = {
    aws = aws.caltrans
  }

  prefix          = "awspd101"
  caltrans_naming = true
  region          = local.region
  environment     = local.environment
  snowflake_storage_integration_iam_user_arn = local.storage_aws_iam_user_arn
  snowflake_storage_integration_external_id  = local.storage_aws_external_id
  snowflake_pipe_sqs_queue_arn               = local.pipe_sqs_queue_arn
}

# Allow ODI account MWAA execution role to access Caltrans pems_raw bucket
resource "aws_s3_bucket_policy" "caltrans_pems_raw_cross_account" {
  provider = aws.caltrans
  bucket   = module.caltrans_s3_lake.pems_raw_bucket.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowODIAccountMWAAAccess"
        Effect = "Allow"
        Principal = {
          AWS = data.aws_iam_role.mwaa_execution_role.arn
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          module.caltrans_s3_lake.pems_raw_bucket.arn,
          "${module.caltrans_s3_lake.pems_raw_bucket.arn}/*"
        ]
      }
    ]
  })
}

############################
# Snowflake Infrastructure #
############################

# Main ELT architecture
module "elt" {
  source = "github.com/cagov/data-infrastructure.git//terraform/snowflake/modules/elt?ref=2477659"
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
  raw_s3_url           = "s3://${module.caltrans_s3_lake.pems_raw_bucket.name}"
  marts_s3_url         = "s3://${module.caltrans_s3_lake.pems_marts_bucket.name}"
  storage_aws_role_arn = module.caltrans_s3_lake.snowflake_storage_integration_role.arn
}

output "pems_raw_stage" {
  value = module.snowflake_clearinghouse.pems_raw_stage
}

output "notification_channel" {
  value = module.snowflake_clearinghouse.notification_channel
}
