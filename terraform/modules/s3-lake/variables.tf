variable "prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "caltrans_naming" {
  description = "Whether to use Caltrans' naming convention for resources"
  type        = bool
  default     = false
}

variable "region" {
  description = "Region for AWS resources"
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "Environment shorthand. Only used with include_resource_types"
  type        = string
  default     = "dev"
}

variable "snowflake_storage_integration_iam_user_arn" {
  description = "ARN for service account created by Snowflake to access external stage"
  type        = string
}

variable "snowflake_storage_integration_external_id" {
  description = "External ID for Snowflake marts storage integration"
  type        = string
  default     = "0000"
}

variable "snowflake_pipe_sqs_queue_arn" {
  description = "SQS Queue ARN for Snowpipe notification channel"
  type        = string
  default     = null
}
