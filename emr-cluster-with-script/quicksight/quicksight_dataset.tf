resource "aws_quicksight_data_set" "dataset" {
  data_set_id=var.data_set_id
  name = var.data_set_name
  import_mode= "SPICE"

  physical_table_map{
    physical_table_map_id = var.physical_table_map_id
    s3_source{
      data_source_arn = aws_quicksight_data_source.datasource.arn
      input_columns {
        name = "Column1"
        type = "STRING"
      }
      upload_settings {
        format = "JSON"
      }
    }
  }
}

resource "aws_iam_role" "quicksight_role" {
  name = var.quicksight_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "quicksight.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "s3_access_policy" {
  name        = var.quicksight_role_policy_name
  description = "Policy to allow QuickSight access to S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = ["s3:GetObject"],
        Effect = "Allow",
        Resource = "${data.aws_s3_bucket.quicksight.arn}/*"
        Condition = {
          StringEqualsIfExists = {
            "aws:RequestedRegion": "us-east-1"
          }
        }
      },
      {
        Action = ["s3:ListBucket"],
        Effect = "Allow",
        Resource = "${data.aws_s3_bucket.quicksight.arn}"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "quicksight_policy_attachment" {
  policy_arn = aws_iam_policy.s3_access_policy.arn
  role       = aws_iam_role.quicksight_role.name
}