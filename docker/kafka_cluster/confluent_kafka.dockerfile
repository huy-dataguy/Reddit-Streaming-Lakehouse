FROM ubuntu:noble

RUN echo "root:root" | chpasswd

RUN apt-get update && \
    apt-get install -y wget -y openjdk-17-jdk vim ssh openssh-server telnet iputils-ping net-tools  python3 python3-pip python3-venv


RUN python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install confluent-kafka pymongo[srv]==3.12 python-dotenv certifi

RUN useradd -m confluent_kafka_user && \
    echo "confluent_kafka_user:kafka" | chpasswd

USER confluent_kafka_user
WORKDIR /home/confluent_kafka_user

# Append rsa_pub to authorized_keys 
# RUN cat config/.ssh/id_rsa.pub >> /home/confluent_kafka_user/.ssh/authorized_keys

COPY --chown=confluent_kafka_user:confluent_kafka_user config/.ssh/* /home/confluent_kafka_user/.ssh/

# USER root
# RUN chmod 700 /home/confluent_kafka_user/.ssh && \
#     chmod 600 /home/confluent_kafka_user/.ssh/id_rsa && \
#     chmod 644 /home/confluent_kafka_user/.ssh/id_rsa.pub && \
#     chmod 600 /home/confluent_kafka_user/.ssh/authorized_keys && \
#     chown -R confluent_kafka_user:confluent_kafka_user /home/confluent_kafka_user/.ssh


COPY config/kafka_cluster/entrypoint2.sh entrypoint.sh
USER root
WORKDIR /home/confluent_kafka_user
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
