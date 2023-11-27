# Outputs
output "pems_raw_stage" {
  value = {
    storage_aws_external_id  = snowflake_storage_integration.pems_raw.storage_aws_external_id
    storage_aws_iam_user_arn = snowflake_storage_integration.pems_raw.storage_aws_iam_user_arn
  }
}
