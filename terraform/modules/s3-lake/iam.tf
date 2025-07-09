##################################
#        IAM Service Users       #
##################################

# NOTE: in general, policies and roles are defined close to the resources
# they support.

# Airflow service user for writing to S3
resource "aws_iam_user" "airflow_s3_writer" {
  name = "${var.prefix}${local.iam_user_infix}-airflow-s3-writer"
}

resource "aws_iam_user_policy_attachment" "airflow_s3_writer_policy_attachment" {
  user       = aws_iam_user.airflow_s3_writer.name
  policy_arn = aws_iam_policy.pems_raw_read_write.arn
}

resource "aws_iam_role_policy_attachment" "snowflake_storage_integration_raw" {
  role       = aws_iam_role.snowflake_storage_integration.name
  policy_arn = aws_iam_policy.pems_raw_external_stage_policy.arn
}

resource "aws_iam_role_policy_attachment" "snowflake_storage_integration_marts" {
  role       = aws_iam_role.snowflake_storage_integration.name
  policy_arn = aws_iam_policy.pems_marts_external_stage_policy.arn
}

# IAM role for Snowflake to assume when reading from the external stage buckets
resource "aws_iam_role" "snowflake_storage_integration" {
  name = "${var.prefix}${local.iam_role_infix}-snowflake-storage-integration"

  # https://docs.snowflake.com/user-guide/data-load-snowpipe-auto-s3#step-5-grant-the-iam-user-permissions-to-access-bucket-objects
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "AWS" : var.snowflake_storage_integration_iam_user_arn
        },
        "Action" : "sts:AssumeRole",
        "Condition" : {
          "StringEquals" : {
            "sts:ExternalId" : var.snowflake_storage_integration_external_id
          }
        }
      }
    ]
    }
  )
}
