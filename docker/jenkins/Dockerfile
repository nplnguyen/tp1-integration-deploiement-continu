FROM jenkins/jenkins:lts-jdk17

USER root

RUN apt-get update \
    && apt-get install -y docker.io docker-compose python3 python3-pip git curl \
    && rm -rf /var/lib/apt/lists/*

RUN python3 --version && docker --version && docker-compose --version

USER jenkins