FROM sparkbase

USER sparkuser
WORKDIR /home/sparkuser


COPY config/spark_on_YARN/master/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml

COPY config/spark_on_YARN/master/workers hadoop/etc/hadoop/workers

USER sparkuser

RUN /home/sparkuser/hadoop/bin/hdfs namenode -format

# COPY config/spark_on_YARN/core-site.xml /home/master_user/hadoop/etc/hadoop/core-site.xml
USER root
COPY config/spark_on_YARN/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]