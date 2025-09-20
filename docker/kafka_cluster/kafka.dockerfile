FROM ubuntu:noble


RUN echo "root:root" | chpasswd

RUN apt-get update && \
    apt-get install -y wget -y openjdk-17-jdk vim ssh openssh-server telnet iputils-ping net-tools 


RUN useradd -m  kafka_user
RUN echo "kafka_user:kafka" | chpasswd

USER kafka_user

WORKDIR /home/kafka_user
# password

RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz
RUN tar -xzf kafka_2.13-3.7.2.tgz
RUN mv kafka_2.13-3.7.2 kafka
RUN rm kafka_2.13-3.7.2.tgz

RUN chown -R kafka_user:kafka_user /home/kafka_user/kafka

RUN echo 'export KAFKA_HOME=/home/kafka_user/kafka'>>~/.bashrc 
RUN echo 'export PATH=$PATH:$KAFKA_HOME/bin'>>~/.bashrc
RUN echo 'export PATH=$PATH:$KAFKA_HOME/config'>>~/.bashrc

RUN echo 'KAFKA_CLUSTER_ID="Q_6ATv-PTJGaFkf27OW8Bg"' >> ~/.bashrc 


# Set environment for entrypoint script

ENV KAFKA_CLUSTER_ID=Q_6ATv-PTJGaFkf27OW8Bg


# Append rsa_pub to authorized_keys 
# RUN cat config/.ssh/id_rsa.pub >> /home/sparkuser/.ssh/authorized_keys
COPY --chown=kafka_user:kafka_user config/.ssh/* /home/kafka_user/.ssh/

COPY config/kafka_cluster/entrypoint.sh entrypoint.sh
USER root
WORKDIR /home/kafka_user
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
