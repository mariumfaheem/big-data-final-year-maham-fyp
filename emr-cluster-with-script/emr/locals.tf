
locals {
  tags = {
    Terraform     = true
    Environment   = var.stage
    Name          = var.name
    Namespace     = var.namespace
    Resource      = "DATA-EMR"
  }
}