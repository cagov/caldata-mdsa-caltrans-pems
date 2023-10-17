##################################
#        IAM Service Users       #
##################################

# NOTE: in general, policies and roles are defined close to the resources
# they support.

# Airflow service user for writing to S3
resource "aws_iam_user" "airflow_s3_writer" {
  name = "${var.prefix}-airflow-s3-writer"
}

resource "aws_iam_user_policy_attachment" "airflow_s3_writer_policy_attachment" {
  user       = aws_iam_user.airflow_s3_writer.name
  policy_arn = aws_iam_policy.pems_raw_write.arn
}
