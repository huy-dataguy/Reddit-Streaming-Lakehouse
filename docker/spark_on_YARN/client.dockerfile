# Base Image
FROM sparkbase

USER root
RUN apt-get update && \
    apt-get install -y python3-pip
    
USER sparkuser
WORKDIR /home/sparkuser

RUN pip3 install --break-system-packages gradio_client

# Copy spark configuration files
COPY --chown=sparkuser:sparkuser config/spark_on_YARN/client/spark-defaults.conf spark/conf/spark-defaults.conf


# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start && su - spark-user && bash"]