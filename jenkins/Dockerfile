# Jenkins with Python (to run the tests) and the Docker CLI (to build images).
FROM jenkins/jenkins:lts-jdk17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv docker-cli openssh-client \
    && rm -rf /var/lib/apt/lists/*
USER jenkins

RUN jenkins-plugin-cli --plugins workflow-aggregator git ssh-agent junit
