"""Small ECR helper for the Jenkins pipeline (uses boto3, already a dependency).

    python ci/ecr.py password <repository>        print a docker login password
    python ci/ecr.py exists <repository> <tag>    exit 0 if the tag is already pushed

<repository> is the full URI: <account>.dkr.ecr.<region>.amazonaws.com/<name>
"""
import base64
import sys

import boto3


def parse(repository: str) -> tuple[str, str]:
    registry, name = repository.split("/", 1)
    return registry.split(".")[3], name  # region, repository name


def main(command: str, repository: str, *args: str) -> int:
    region, name = parse(repository)
    ecr = boto3.client("ecr", region_name=region)
    if command == "password":
        print(password(ecr))
        return 0
    if command == "exists":
        try:
            ecr.describe_images(repositoryName=name, imageIds=[{"imageTag": args[0]}])
            return 0
        except ecr.exceptions.ImageNotFoundException:
            return 1
    raise SystemExit(__doc__)


def password(ecr) -> str:
    token = ecr.get_authorization_token()["authorizationData"][0]["authorizationToken"]
    return base64.b64decode(token).decode().split(":", 1)[1]


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
