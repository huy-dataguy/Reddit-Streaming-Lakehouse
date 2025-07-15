FROM ubuntu:noble

RUN apt-get update \
    && apt-get install -y wget -y openjdk-17-jdk iproute2 netcat-openbsd

RUN useradd -m zookeeper-u

WORKDIR /home/zookeeper-u
# password

RUN wget https://dlcdn.apache.org/zookeeper/current/apache-zookeeper-3.9.3-bin.tar.gz
RUN tar -xzf apache-zookeeper-3.9.3-bin.tar.gz
RUN mv apache-zookeeper-3.9.3-bin zookeeper



CMD ["/bin/bash"]


