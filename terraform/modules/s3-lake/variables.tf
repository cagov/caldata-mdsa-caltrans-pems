variable "prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "region" {
  description = "Region for AWS resources"
  type        = string
  default     = "us-west-2"
}

variable "snowflake_raw_storage_integration_iam_user_arn" {
  description = "ARN for service account created by Snowflake to access external stage"
  type        = string
}

variable "snowflake_raw_storage_integration_external_id" {
  description = "External ID for Snowflake storage integration"
  type        = string
  default     = "0000"
}
