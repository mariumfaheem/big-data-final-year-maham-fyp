name="aml-emr"
cidr="10.0.0.0/16"
stage="prod"
namespace="default"
private_subnets =["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
//["subnet-01d33c7e156d73922"]
public_subnets=["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
//["subnet-042fcb53514f28243","subnet-04f86a579b6b20465"]
availability_zones= ["eu-west-1a","eu-west-1b","eu-west-1c"]
region="eu-west-1"
key="aml-emr"
core_instance_group_instance_type="r5.xlarge"
core_instance_group_instance_count="2"
core_instance_group_ebs_size="50"
core_instance_group_ebs_type="standard"
core_instance_group_ebs_volumes_per_instance="2"
task_instance_group_instance_type="r5.xlarge"
task_instance_group_instance_count="2"
task_instance_group_ebs_size="50"
task_instance_group_ebs_type="standard"
task_instance_group_ebs_volumes_per_instance= "2"
create_task_instance_group="0"
master_instance_group_instance_type="r5.xlarge"
master_instance_group_instance_count= "1"
master_instance_group_ebs_size= "50"
master_instance_group_ebs_type= "standard"
master_instance_group_ebs_volumes_per_instance= "2"
log_bucket="arn:aws:s3:::aml-emr-logs"
