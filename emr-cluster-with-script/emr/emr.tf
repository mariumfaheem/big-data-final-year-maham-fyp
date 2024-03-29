
module "emr_cluster" {

  source = "cloudposse/emr-cluster/aws"
  master_allowed_security_groups                 = [module.vote_service_sg.security_group_id]
  slave_allowed_security_groups                  = [module.vote_service_sg.security_group_id]
  managed_master_security_group                  = module.vote_service_sg.security_group_id
  managed_slave_security_group                   = module.vote_service_sg.security_group_id
  use_existing_managed_master_security_group     = false
  use_existing_managed_slave_security_group      = false
  region                                         = var.region
  vpc_id                                         = module.vpc.vpc_id
  subnet_id                                      = var.enabled ? module.vpc.public_subnets[0] : null
  route_table_id                                 = var.enabled ? module.vpc.default_route_table_id : null
  version = "0.20.0"
  namespace = var.namespace
  stage = var.stage
  name = "${var.name}-emr"
  subnet_type = "public"
  ebs_root_volume_size = 50
  visible_to_all_users = true
  release_label = "emr-6.15.0"


  applications = [
    "Hive",
    "Pig",
    "JupyterHub",
    "JupyterEnterpriseGateway",
    "Spark",
    "livy",
  ]
  core_instance_group_instance_type = var.core_instance_group_instance_type
  core_instance_group_instance_count = var.core_instance_group_instance_count
  core_instance_group_ebs_size = var.core_instance_group_ebs_size
  core_instance_group_ebs_type = var.core_instance_group_ebs_type
  core_instance_group_ebs_volumes_per_instance = var.core_instance_group_ebs_volumes_per_instance
  master_instance_group_instance_type = var.master_instance_group_instance_type
  master_instance_group_instance_count = var.master_instance_group_instance_count
  master_instance_group_ebs_size = var.master_instance_group_ebs_size
  master_instance_group_ebs_type = var.master_instance_group_ebs_type
  master_instance_group_ebs_volumes_per_instance = var.master_instance_group_ebs_volumes_per_instance
  create_task_instance_group = var.create_task_instance_group
  task_instance_group_instance_type = var.task_instance_group_instance_type
  task_instance_group_instance_count = var.task_instance_group_instance_count
  task_instance_group_ebs_size = var.task_instance_group_ebs_size
  task_instance_group_ebs_type = var.task_instance_group_ebs_type
  task_instance_group_ebs_volumes_per_instance = var.task_instance_group_ebs_volumes_per_instance
  task_instance_group_bid_price = "0.18"
  log_uri                                        = format("s3n://%s/", data.aws_s3_bucket.logs_bucket_name.id)
   tags = local.tags
  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true




}
