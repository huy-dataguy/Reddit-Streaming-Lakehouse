FROM sparkbase

USER sparkuser
WORKDIR /home/sparkuser


RUN cp spark/yarn/spark-*-yarn-shuffle.jar hadoop/share/hadoop/yarn/lib/

COPY config/spark_on_YARN/worker/core-site.xml hadoop/etc/hadoop/core-site.xml
COPY config/spark_on_YARN/worker/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
COPY config/spark_on_YARN/worker/yarn-site.xml hadoop/etc/hadoop/yarn-site.xml

USER root


COPY config/spark_on_YARN/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]