FROM ubuntu:jammy

RUN apt-get update && apt-get -y install jq wget gnupg apt-transport-https ca-certificates python3-pip python3 bash curl tcpdump tcpflow netcat
RUN pip3 install awscurl awscli

RUN mkdir /opt/goshenite
COPY ./goshenite-writer /opt/goshenite/goshenite-writer
COPY ./writer-config.yaml /opt/goshenite/writer-config.yaml

EXPOSE 2003/tcp

ENTRYPOINT ["/opt/goshenite/goshenite-writer", "-c", "/opt/goshenite/writer-config.yaml"]
