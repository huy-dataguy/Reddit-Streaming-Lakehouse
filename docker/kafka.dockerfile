FROM ubuntu:noble

RUN apt-get update \
    && apt-get install -y wget -y openjdk-17-jdk

RUN useradd -m  kafka-u

WORKDIR /home/kafka-u
# password

RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz
RUN tar -xzf kafka_2.13-3.7.2.tgz
RUN mv kafka_2.13-3.7.2 kafka

RUN apt-get install -y vim

CMD ["/bin/bash"]