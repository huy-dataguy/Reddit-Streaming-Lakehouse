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

RUN echo 'export KAFKA_HOME=/home/kafka_user/kafka'>>~/.bashrc 
RUN echo 'export PATH=$PATH:$KAFKA_HOME/bin'>>~/.bashrc
RUN echo 'export PATH=$PATH:$KAFKA_HOME/config'>>~/.bashrc

RUN echo 'KAFKA_CLUSTER_ID="Q_6ATv-PTJGaFkf27OW8Bg"' >> ~/.bashrc 


CMD ["/bin/bash"]