##################################
#  Caltrans PeMS Infrastructure  #
##################################

############
# PeMS raw #
############

resource "aws_s3_bucket" "pems_raw" {
  bucket = "${var.prefix}${local.s3_bucket_infix}-${var.region}-raw"
}

# Versioning
resource "aws_s3_bucket_versioning" "pems_raw" {
  bucket = aws_s3_bucket.pems_raw.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

# Write access
data "aws_iam_policy_document" "pems_raw_read_write" {
  statement {
    actions = [
      "s3:ListBucket"
    ]
    resources = [aws_s3_bucket.pems_raw.arn]
  }
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
    ]
    resources = [
      aws_s3_bucket.pems_raw.arn,
      "${aws_s3_bucket.pems_raw.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "pems_raw_read_write" {
  name        = "${var.caltrans_naming ? "custom_" : ""}${var.prefix}${local.iam_policy_infix}-${var.region}-raw-read-write"
  description = "Policy allowing read/write for s3 pems raw bucket"
  policy      = data.aws_iam_policy_document.pems_raw_read_write.json
}

resource "aws_s3_bucket_public_access_block" "pems_raw" {
  bucket = aws_s3_bucket.pems_raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# External stage policy
# From https://docs.snowflake.com/user-guide/data-load-snowpipe-auto-s3#creating-an-iam-policy
data "aws_iam_policy_document" "pems_raw_external_stage_policy" {
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [aws_s3_bucket.pems_raw.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["*"]
    }

  }
  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.pems_raw.arn}/*"]
  }
}

resource "aws_iam_policy" "pems_raw_external_stage_policy" {
  name        = "${var.caltrans_naming ? "custom_" : ""}${var.prefix}${local.iam_policy_infix}-${var.region}-pems-raw-external-stage-policy"
  description = "Policy allowing read/write for snowpipe-test bucket"
  policy      = data.aws_iam_policy_document.pems_raw_external_stage_policy.json
}

# Snowpipe notifications
resource "aws_s3_bucket_notification" "snowflake_pipe_notifications" {
  count  = var.snowflake_pipe_sqs_queue_arn == null ? 0 : 1
  bucket = aws_s3_bucket.pems_raw.id
  queue {
    queue_arn = var.snowflake_pipe_sqs_queue_arn
    events    = ["s3:ObjectCreated:*"]
  }
}

##############
# PeMS marts #
##############

resource "aws_s3_bucket" "pems_marts" {
  bucket = "${var.prefix}${local.s3_bucket_infix}-${var.region}-marts"
}

# Versioning
resource "aws_s3_bucket_versioning" "pems_marts" {
  bucket = aws_s3_bucket.pems_marts.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "pems_marts" {
  bucket = aws_s3_bucket.pems_marts.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# External stage policy
# https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#creating-an-iam-policy
data "aws_iam_policy_document" "pems_marts_external_stage_policy" {
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [aws_s3_bucket.pems_marts.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["*"]
    }

  }
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion"
    ]
    resources = ["${aws_s3_bucket.pems_marts.arn}/*"]
  }
}

resource "aws_iam_policy" "pems_marts_external_stage_policy" {
  name        = "${var.caltrans_naming ? "custom_" : ""}${var.prefix}${local.iam_policy_infix}-${var.region}-pems-marts-external-stage-policy"
  description = "Policy allowing read/write for PeMS marts bucket"
  policy      = data.aws_iam_policy_document.pems_marts_external_stage_policy.json
}

data "aws_iam_policy_document" "pems_marts_public_read" {
  statement {
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.pems_marts.arn,
      "${aws_s3_bucket.pems_marts.arn}/*",
    ]
  }
}

resource "aws_s3_bucket_policy" "pems_marts_public_read" {
  bucket = aws_s3_bucket.pems_marts.id
  policy = data.aws_iam_policy_document.pems_marts_public_read.json
}
