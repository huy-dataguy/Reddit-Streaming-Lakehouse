FROM apache/airflow:2.8.1

USER root

# Install Docker CLI inside the container
RUN apt-get update -qq && apt-get install -y docker.io 

COPY --chown=airflow:airflow ../../config/ssh/* /home/airflow/.ssh/

USER airflow