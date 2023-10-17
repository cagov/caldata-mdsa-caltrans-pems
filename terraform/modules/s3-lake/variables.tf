variable "prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "region" {
  description = "Region for AWS resources"
  type        = string
  default     = "us-west-2"
}
