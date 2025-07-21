FROM ubuntu:noble

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv

RUN python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install confluent-kafka

RUN useradd -m confluent_kafka_user && \
    echo "confluent_kafka_user:kafka" | chpasswd

USER confluent_kafka_user
WORKDIR /home/confluent_kafka_user

CMD ["/bin/bash"]
