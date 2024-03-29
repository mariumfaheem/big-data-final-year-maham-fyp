provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "s3" {
  bucket = var.bucket
  tags = var.mandatory_tags
}

resource "aws_s3_bucket_versioning" "versioning" {
  count  = var.versioning_is_enabled ? 1 : 0
  bucket = aws_s3_bucket.s3.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

# resource "aws_s3_bucket_acl" "s3_private" {
#   bucket = aws_s3_bucket.s3.id
#   acl    = "private"
# }

resource "aws_s3_bucket_object" "s3_objects" {
  count  = var.create_dir ? length(var.s3_folders) : 0
  bucket = aws_s3_bucket.s3.bucket
  acl    = "private"
  key    = "${var.s3_folders[count.index]}/"
  #   source = "/dev/null"
}