FROM sparkbase

USER root
RUN apt-get update && \
    apt-get install -y python3-pip
RUN pip3 install --break-system-packages gradio_client

USER sparkuser
WORKDIR /home/sparkuser


RUN cp spark/yarn/spark-*-yarn-shuffle.jar hadoop/share/hadoop/yarn/lib/

COPY --chown=sparkuser:sparkuser config/spark_on_YARN/worker/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml

USER root


COPY config/spark_on_YARN/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]