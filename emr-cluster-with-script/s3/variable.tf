variable "bucket" {
  type = string
}

variable "s3_folders" {
  type        = list(any)
  description = "The list of S3 folders to create"
  default     = [""]
}

variable "create_dir" {
  type    = bool
  default = false
}

//variable "tags" {
//  description = "Resource tags"
//  type        = map(string)
//  default     = {}
//}

variable "mandatory_tags" {
  type = map(string)
}

variable "name" {
  description = "The name of the DB subnet group"
  type        = string
  default     = ""
}

variable "versioning_is_enabled" {
  type    = bool
  default = false
}

variable "bucket_is_encrypted" {
  type    = bool
  default = false
}
variable "region" {
  type=string
}