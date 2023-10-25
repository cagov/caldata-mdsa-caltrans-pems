##################################
#  Caltrans PeMS Infrastructure  #
##################################

# PeMS raw
resource "aws_s3_bucket" "pems_raw" {
  bucket = "${var.prefix}-${var.region}-raw"
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
  name        = "${var.prefix}-${var.region}-raw-read-write"
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
