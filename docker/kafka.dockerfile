FROM ubuntu:noble


RUN echo "root:root" | chpasswd

RUN apt-get update \
    && apt-get install -y wget -y openjdk-17-jdk vim

RUN useradd -m  kafka-u
RUN echo "kafka-u:kafka" | chpasswd

USER kafka-u

WORKDIR /home/kafka-u
# password

RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz
RUN tar -xzf kafka_2.13-3.7.2.tgz
RUN mv kafka_2.13-3.7.2 kafka

RUN echo 'export KAFKA_HOME=/home/kafka-u/kafka'>>~/.bashrc 
RUN echo 'export PATH=$PATH:$KAFKA_HOME/bin'>>~/.bashrc
RUN echo 'export PATH=$PATH:$KAFKA_HOME/config'>>~/.bashrc

USER root
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y ssh openssh-server telnet iputils-ping \
    net-tools && \
    apt-get clean

USER kafka-u 

RUN mkdir -p /home/kafka-u/.ssh && \
    ssh-keygen -t rsa -P '' -f /home/kafka-u/.ssh/id_rsa && \
    cat /home/kafka-u/.ssh/id_rsa.pub >> /home/kafka-u/.ssh/authorized_keys && \
    chmod 600 /home/kafka-u/.ssh/authorized_keys && \
    chown -R kafka-u:kafka-u /home/kafka-u/.ssh

RUN echo 'KAFKA_CLUSTER_ID="Q_6ATv-PTJGaFkf27OW8Bg"' >> ~/.bashrc 


CMD ["/bin/bash"]