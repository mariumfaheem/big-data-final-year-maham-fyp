resource "aws_quicksight_data_source" "datasource" {
  data_source_id = var.data_source_id
  name           = var.data_source_name
  type           = var.data_source_type #s3
  parameters {
    s3 {
      manifest_file_location {
        bucket = var.s3_manifest_bucket_name
        key    = "manifest/manifest.json"
      }
    }
  }

}
