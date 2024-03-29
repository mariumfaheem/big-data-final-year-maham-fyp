variable "name" {
  type        = string
  description = "Cluster Name"
}

variable "cidr" {}
variable "availability_zones" {
  type        = list(string)
  description = "List of availability zones"
}

variable "enabled" {
  type = string
  default = true
}


variable "stage" {
  type        = string
  description = "Cluster Stage"
}

variable "namespace" {
  type        = string
  description = "Cluster Name"
}

variable "private_subnets" {
  type        = list(string)
}

variable "public_subnets" {
  type        = list(string)

}


variable "region" {
  type        = string
  description = "AWS region"
}

variable "key" {
  type = string
}


variable "core_instance_group_instance_type" {
  type        = string
  description = "EC2 instance type for all instances in the Core instance group"
}

variable "core_instance_group_instance_count" {
  type        = number
  description = "Target number of instances for the Core instance group. Must be at least 1"
}

variable "core_instance_group_ebs_size" {
  type        = number
  description = "Core instances volume size, in gibibytes (GiB)"
}

variable "core_instance_group_ebs_type" {
  type        = string
  description = "Core instances volume type. Valid options are `gp2`, `io1`, `standard` and `st1`"
}

variable "core_instance_group_ebs_volumes_per_instance" {
  type        = number
  description = "The number of EBS volumes with this configuration to attach to each EC2 instance in the Core instance group"
}

variable "task_instance_group_instance_type" {
  type        = string
  description = "EC2 instance type for all instances in the TASK instance group"
}

variable "task_instance_group_instance_count" {
  type        = number
  description = "Target number of instances for the TASk instance group. Must be at least 1"
}

variable "task_instance_group_ebs_size" {
  type        = number
  description = "TASK instances volume size, in gibibytes (GiB)"
}

variable "task_instance_group_ebs_type" {
  type        = string
  description = "TASK instances volume type. Valid options are `gp2`, `io1`, `standard` and `st1`"
}

variable "task_instance_group_ebs_volumes_per_instance" {
  type        = number
  description = "The number of EBS volumes with this configuration to attach to each EC2 instance in the TASK instance group"
}

variable "create_task_instance_group" {
  type        = bool
  description = "Whether to create an instance group for Task nodes. For more info: https://www.terraform.io/docs/providers/aws/r/emr_instance_group.html, https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-master-core-task-nodes.html"
}

variable "master_instance_group_instance_type" {
  type        = string
  description = "EC2 instance type for all instances in the Master instance group"
}

variable "master_instance_group_instance_count" {
  type        = number
  description = "Target number of instances for the Master instance group. Must be at least 1"
}

variable "master_instance_group_ebs_size" {
  type        = number
  description = "Master instances volume size, in gibibytes (GiB)"
}

variable "master_instance_group_ebs_type" {
  type        = string
  description = "Master instances volume type. Valid options are `gp2`, `io1`, `standard` and `st1`"
}

variable "master_instance_group_ebs_volumes_per_instance" {
  type        = number
  description = "The number of EBS volumes with this configuration to attach to each EC2 instance in the Master instance group"
}

variable "log_bucket" {
  type = string

}
