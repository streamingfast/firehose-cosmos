# syntax=docker/dockerfile:1.2

FROM ghcr.io/streamingfast/firehose-core:v1.12.4 as core

FROM ubuntu:24.04

# gettext-base is used for envsubst
RUN apt-get update && apt-get -y install ca-certificates htop iotop sysstat strace lsof curl jq tzdata file gettext-base &&  rm -rf /var/cache/apt /var/lib/apt/lists/*
RUN rm /etc/localtime && ln -snf /usr/share/zoneinfo/America/Montreal /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

ADD /fireinjective/fireinjective /app/fireinjective
ADD /firemantra/firemantra /app/firemantra

ENV PATH "$PATH:/app"

COPY --from=core /app/firecore /app/firecore

ENTRYPOINT [""]
