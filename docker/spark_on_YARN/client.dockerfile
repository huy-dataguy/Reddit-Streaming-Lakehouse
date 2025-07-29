# Base Image
FROM sparkbase

USER sparkuser
WORKDIR /home/sparkuser

# Copy spark configuration files
COPY config/spark_on_YARN/client/spark-defaults.conf spark/conf/spark-defaults.conf


# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start && su - spark-user && bash"]