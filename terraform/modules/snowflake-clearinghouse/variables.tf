variable "environment" {
  description = "Environment suffix"
  type        = string
}

variable "raw_s3_url" {
  description = "S3 URL for the storage integration"
  type        = string
}

variable "marts_s3_url" {
  description = "S3 URL for the storage integration"
  type        = string
}

variable "storage_aws_role_arn" {
  description = "ARN of IAM role for Snowflake to assume with access to s3 storage"
  type        = string
}
