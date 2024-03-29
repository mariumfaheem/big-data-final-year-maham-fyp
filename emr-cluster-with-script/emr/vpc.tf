module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  name = "${var.name}-${var.namespace}-${var.stage}-vpc"
  cidr = var.cidr


  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  azs              =var.availability_zones
  #private_subnets  = var.private_subnets
  #public_subnets   = var.public_subnets

  create_database_subnet_group = false

  manage_default_route_table = false
  default_route_table_tags   = { DefaultRouteTable = true }

  enable_dns_hostnames = true
  enable_dns_support   = true

  #enable_classiclink             = true
  #enable_classiclink_dns_support = true

  enable_nat_gateway     = true
  single_nat_gateway     = true
  one_nat_gateway_per_az = false

  enable_vpn_gateway = true

  enable_dhcp_options = true

  manage_default_security_group = true
  default_security_group_ingress = [
    {
      from_port = 0
      to_port   = 0
      protocol  = -1
      cidr_blocks = "0.0.0.0/0"
    }

  ]
  default_security_group_egress = [
    {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      cidr_blocks = "0.0.0.0/0"
    }
  ]
  enable_flow_log                      = false
  create_flow_log_cloudwatch_log_group = false
  create_flow_log_cloudwatch_iam_role  = false
  flow_log_max_aggregation_interval    = 60

  tags = local.tags
  default_security_group_tags={Name="vpc-managed"}
}
