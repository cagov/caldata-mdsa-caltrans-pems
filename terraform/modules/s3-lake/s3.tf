##################################
#  Caltrans PeMS Infrastructure  #
##################################

# PeMS raw
resource "aws_s3_bucket" "pems_raw" {
  bucket = "${var.prefix}-${var.region}-pems-raw"
}

# Versioning
resource "aws_s3_bucket_versioning" "pems_raw" {
  bucket = aws_s3_bucket.pems_raw.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

# Write access
data "aws_iam_policy_document" "pems_raw_write" {
  statement {
    actions = [
      "s3:ListBucket"
    ]
    resources = [aws_s3_bucket.pems_raw.arn]
  }
  statement {
    actions = [
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.pems_raw.arn}/*"]
  }
}

resource "aws_iam_policy" "pems_raw_write" {
  name        = "${var.prefix}-${var.region}-pems-raw-write"
  description = "Policy allowing write for s3 pems raw bucket"
  policy      = data.aws_iam_policy_document.pems_raw_write.json
}


# Public read access
data "aws_iam_policy_document" "pems_raw_read" {
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
      aws_s3_bucket.pems_raw.arn,
      "${aws_s3_bucket.pems_raw.arn}/*",
    ]
  }
}

resource "aws_s3_bucket_policy" "pems_raw_read" {
  bucket = aws_s3_bucket.pems_raw.id
  policy = data.aws_iam_policy_document.pems_raw_read.json
}

resource "aws_s3_bucket_public_access_block" "pems_raw" {
  bucket = aws_s3_bucket.pems_raw.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}
