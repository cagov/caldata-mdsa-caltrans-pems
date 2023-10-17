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
  }

  backend "s3" {
  }
}

locals {
  owner       = "caltrans"
  environment = "dev"
  project     = "pems"
  region      = "us-west-2"
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

############################
#      Infrastructure      #
############################

module "s3_lake" {
  source = "../../modules/s3-lake"

  prefix = "${local.owner}-${local.project}-${local.environment}"
  region = local.region
}
