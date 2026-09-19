"""Creates the AWS infrastructure for LedgerFlow with boto3 and starts the app on EC2.

Needs AWS credentials on your laptop (for example from `aws configure`).

    python provision_ec2.py           # create everything and print the URL
    python provision_ec2.py destroy   # terminate the server and release its IP to stop charges

What it creates:
  - S3 bucket, SQS queues and CloudWatch alarm (via setup_aws.py)
  - IAM role for the server with only the permissions the app needs
  - SSH key pair and a security group (HTTP open, SSH only from your current IP)
  - one EC2 instance (Amazon Linux 2023) with a fixed Elastic IP
The server installs Docker on first boot, clones the repo and runs deploy.sh.
"""
import json
import secrets
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

import setup_aws
from aws_services import AWS_REGION, METRICS_NAMESPACE, SQS_QUEUE_NAME

NAME = "ledgerflow"
INSTANCE_TYPE = "t4g.small"  # 2 vCPU, 2 GB RAM, ARM (Graviton)
REPO_URL = "https://github.com/anuranganathan/LedgerFlow.git"
KEY_PATH = Path.home() / ".ssh" / f"{NAME}-key.pem"
AMI_PARAMETER = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"

ec2 = boto3.client("ec2", region_name=AWS_REGION)
iam = boto3.client("iam")
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
# S3 bucket names are global across all AWS accounts, so the account ID keeps ours unique.
BUCKET = f"{NAME}-receipts-{ACCOUNT_ID}"


def create_iam_role() -> None:
    """The app gets temporary credentials from this role instead of stored access keys."""
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"},
                       "Action": "sts:AssumeRole"}],
    }
    # Least privilege: only the exact actions and resources LedgerFlow uses.
    permissions = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
             "Resource": f"arn:aws:s3:::{BUCKET}/*"},
            {"Effect": "Allow",
             "Action": ["sqs:GetQueueUrl", "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage"],
             "Resource": f"arn:aws:sqs:{AWS_REGION}:{ACCOUNT_ID}:{SQS_QUEUE_NAME}"},
            {"Effect": "Allow", "Action": "cloudwatch:PutMetricData", "Resource": "*",
             "Condition": {"StringEquals": {"cloudwatch:namespace": METRICS_NAMESPACE}}},
            {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": f"arn:aws:logs:{AWS_REGION}:{ACCOUNT_ID}:log-group:{setup_aws.LOG_GROUP}:*"},
        ],
    }
    role = f"{NAME}-ec2-role"
    try:
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=json.dumps(trust_policy))
        iam.create_instance_profile(InstanceProfileName=role)
        iam.add_role_to_instance_profile(InstanceProfileName=role, RoleName=role)
    except iam.exceptions.EntityAlreadyExistsException:
        pass
    iam.put_role_policy(RoleName=role, PolicyName=f"{NAME}-app", PolicyDocument=json.dumps(permissions))
    print(f"IAM role {role} ready")


def create_key_pair() -> None:
    if KEY_PATH.exists():
        print(f"Using existing SSH key {KEY_PATH}")
        return
    try:
        ec2.delete_key_pair(KeyName=f"{NAME}-key")  # the private key was lost, so replace it
    except ClientError:
        pass
    key = ec2.create_key_pair(KeyName=f"{NAME}-key", KeyType="ed25519")
    KEY_PATH.parent.mkdir(exist_ok=True)
    KEY_PATH.write_text(key["KeyMaterial"])
    KEY_PATH.chmod(0o600)
    print(f"Saved SSH key to {KEY_PATH}")


def create_security_group() -> str:
    vpc_id = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    existing = ec2.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [f"{NAME}-sg"]}, {"Name": "vpc-id", "Values": [vpc_id]}]
    )["SecurityGroups"]
    if existing:
        group_id = existing[0]["GroupId"]
    else:
        group_id = ec2.create_security_group(
            GroupName=f"{NAME}-sg", Description="LedgerFlow: HTTP from anywhere, SSH from admin IP",
            VpcId=vpc_id,
        )["GroupId"]
    my_ip = urllib.request.urlopen("https://checkip.amazonaws.com").read().decode().strip()
    for port, cidr in [(80, "0.0.0.0/0"), (22, f"{my_ip}/32")]:
        try:
            ec2.authorize_security_group_ingress(
                GroupId=group_id, IpProtocol="tcp", FromPort=port, ToPort=port, CidrIp=cidr
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                raise
    print(f"Security group {group_id} ready (SSH allowed from {my_ip})")
    return group_id


def user_data() -> str:
    """Shell script the server runs once on its first boot."""
    return f"""#!/bin/bash
set -eux
dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL https://github.com/docker/compose/releases/download/v2.39.2/docker-compose-linux-aarch64 \\
  -o /usr/local/lib/docker/cli-plugins/docker-compose
curl -fsSL https://github.com/docker/buildx/releases/download/v0.26.1/buildx-v0.26.1.linux-arm64 \\
  -o /usr/local/lib/docker/cli-plugins/docker-buildx
chmod +x /usr/local/lib/docker/cli-plugins/*

# 2 GB of swap so Kafka, PostgreSQL and the app fit comfortably in 2 GB of RAM
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile swap swap defaults 0 0' >> /etc/fstab

sudo -u ec2-user git clone {REPO_URL} /home/ec2-user/LedgerFlow
cat > /home/ec2-user/LedgerFlow/.env <<EOF
AWS_REGION={AWS_REGION}
S3_BUCKET={BUCKET}
POSTGRES_PASSWORD={secrets.token_urlsafe(24)}
SLACK_WEBHOOK_URL=
EOF
chown ec2-user:ec2-user /home/ec2-user/LedgerFlow/.env
chmod 600 /home/ec2-user/LedgerFlow/.env
sudo -u ec2-user /home/ec2-user/LedgerFlow/deploy.sh
"""


def find_instance() -> dict | None:
    reservations = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": [f"{NAME}-server"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ])["Reservations"]
    return reservations[0]["Instances"][0] if reservations else None


def find_elastic_ip() -> dict | None:
    addresses = ec2.describe_addresses(Filters=[{"Name": "tag:Name", "Values": [f"{NAME}-ip"]}])["Addresses"]
    return addresses[0] if addresses else None


def create_instance(group_id: str) -> str:
    instance = find_instance()
    if instance:
        print(f"Server {instance['InstanceId']} already exists")
        return instance["InstanceId"]
    ami_id = boto3.client("ssm", region_name=AWS_REGION).get_parameter(Name=AMI_PARAMETER)["Parameter"]["Value"]
    for attempt in range(10):
        try:
            instance = ec2.run_instances(
                ImageId=ami_id, InstanceType=INSTANCE_TYPE, MinCount=1, MaxCount=1,
                KeyName=f"{NAME}-key", SecurityGroupIds=[group_id],
                IamInstanceProfile={"Name": f"{NAME}-ec2-role"},
                UserData=user_data(),
                # IMDSv2 only; hop limit 2 lets containers reach the role credentials.
                MetadataOptions={"HttpTokens": "required", "HttpPutResponseHopLimit": 2},
                BlockDeviceMappings=[{"DeviceName": "/dev/xvda",
                                      "Ebs": {"VolumeSize": 16, "VolumeType": "gp3"}}],
                TagSpecifications=[{"ResourceType": "instance",
                                    "Tags": [{"Key": "Name", "Value": f"{NAME}-server"}]}],
            )["Instances"][0]
            break
        except ClientError as exc:
            # A brand-new IAM role takes a few seconds before EC2 can see it.
            if "Invalid IAM Instance Profile" not in str(exc) or attempt == 9:
                raise
            time.sleep(5)
    print(f"Launching server {instance['InstanceId']}...")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance["InstanceId"]])
    return instance["InstanceId"]


def attach_elastic_ip(instance_id: str) -> str:
    address = find_elastic_ip()
    if address is None:
        address = ec2.allocate_address(Domain="vpc", TagSpecifications=[
            {"ResourceType": "elastic-ip", "Tags": [{"Key": "Name", "Value": f"{NAME}-ip"}]}
        ])
    ec2.associate_address(AllocationId=address["AllocationId"], InstanceId=instance_id)
    return address["PublicIp"]


def provision() -> None:
    setup_aws.S3_BUCKET = BUCKET
    setup_aws.create_bucket()
    setup_aws.create_queues()
    setup_aws.create_monitoring()
    create_iam_role()
    create_key_pair()
    group_id = create_security_group()
    instance_id = create_instance(group_id)
    ip = attach_elastic_ip(instance_id)
    print(f"""
Done. The first boot installs Docker and builds the app, which takes about 5-8 minutes.
  App:       http://{ip}
  API docs:  http://{ip}/docs
  SSH:       ssh -i {KEY_PATH} ec2-user@{ip}
  Boot log:  ssh -i {KEY_PATH} ec2-user@{ip} sudo tail -f /var/log/cloud-init-output.log""")


def destroy() -> None:
    instance = find_instance()
    if instance:
        ec2.terminate_instances(InstanceIds=[instance["InstanceId"]])
        print(f"Terminating {instance['InstanceId']}...")
        ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance["InstanceId"]])
    address = find_elastic_ip()
    if address:
        ec2.release_address(AllocationId=address["AllocationId"])
        print(f"Released {address['PublicIp']}")
    print("Server removed. S3, SQS, IAM and the security group are kept (they cost nothing when idle).")


if __name__ == "__main__":
    destroy() if sys.argv[1:] == ["destroy"] else provision()
