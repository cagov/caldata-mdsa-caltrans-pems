##################################
#        Terraform Setup         #
##################################

terraform {
  # Note: when a package is added or updated, we have to update the lockfile in a
  # platform-independent way, cf. https://github.com/hashicorp/terraform/issues/28041
  # To update the lockfile run:
  #
  # terraform providers lock -platform=linux_amd64 -platform=darwin_amd64
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.56.0"
    }
  }
  required_version = ">= 1.0"
}

# Caltrans has a naming convention where a three-letter resource type is in the name
# of the resources. This creates an infix string which satisfies that convention
# when combined with var.prefix
locals {
  iam_user_infix   = "${var.caltrans_naming ? "iau" : ""}${var.caltrans_naming ? "-" : ""}${var.caltrans_naming ? var.environment : ""}"
  iam_role_infix   = "${var.caltrans_naming ? "iar" : ""}${var.caltrans_naming ? "-" : ""}${var.caltrans_naming ? var.environment : ""}"
  iam_policy_infix = "${var.caltrans_naming ? "iap" : ""}${var.caltrans_naming ? "-" : ""}${var.caltrans_naming ? var.environment : ""}"
  s3_bucket_infix = "${var.caltrans_naming ? "s3b" : ""}${var.caltrans_naming ? "-" : ""}${var.caltrans_naming ? var.environment : ""}"
}
