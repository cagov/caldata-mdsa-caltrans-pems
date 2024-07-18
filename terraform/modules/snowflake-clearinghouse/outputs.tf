# Outputs
output "pems_raw_stage" {
  value = {
    storage_aws_external_id  = snowflake_storage_integration.pems_storage_integration.storage_aws_external_id
    storage_aws_iam_user_arn = snowflake_storage_integration.pems_storage_integration.storage_aws_iam_user_arn
  }
}

output "notification_channel" {
  description = "ARN of the notification channel for pipes"
  # All notification channels for the same bucket are the same.
  value = snowflake_pipe.station_raw_pipe.notification_channel
}
