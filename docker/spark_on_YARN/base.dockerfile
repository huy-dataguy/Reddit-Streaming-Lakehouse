FROM ubuntu:noble

RUN echo "root:root" | chpasswd

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget ssh openssh-server vim sudo telnet iputils-ping curl zip unzip
    
RUN useradd -m  sparkuser
RUN echo "sparkuser:spark" | chpasswd


USER sparkuser

WORKDIR /home/sparkuser

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xzf hadoop-3.4.1.tar.gz && \
    mv hadoop-3.4.1 hadoop && \
    rm hadoop-3.4.1.tar.gz

RUN wget https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-without-hadoop.tgz && \
    tar -xzf spark-3.5.6-bin-without-hadoop.tgz && \
    mv spark-3.5.6-bin-without-hadoop spark && \
    rm spark-3.5.6-bin-without-hadoop.tgz


RUN curl -s "https://get.sdkman.io" | bash && \
    echo 'source "/home/sparkuser/.sdkman/bin/sdkman-init.sh"' >> /home/sparkuser/.bashrc && \
    bash -c "source /home/sparkuser/.sdkman/bin/sdkman-init.sh && sdk install scala 2.13.16"

RUN mkdir -p /home/sparkuser/.ssh && \
    ssh-keygen -t rsa -P '' -f /home/sparkuser/.ssh/id_rsa && \
    cat /home/sparkuser/.ssh/id_rsa.pub >> /home/sparkuser/.ssh/authorized_keys && \
    chmod 600 /home/sparkuser/.ssh/authorized_keys && \
    chown -R sparkuser:sparkuser /home/sparkuser/.ssh

RUN echo 'export HADOOP_HOME=/home/sparkuser/hadoop'>>~/.bashrc
RUN echo 'export SPARK_HOME=/home/sparkuser/spark'>>~/.bashrc
RUN echo 'export PATH=$PATH:$HADOOP_HOME/sbin' >> ~/.bashrc
RUN echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc

RUN echo 'export PATH=$PATH:$SPARK_HOME/bin'>>~/.bashrc
RUN echo 'export PATH="$SPARK_HOME/sbin:$PATH"'>>~/.bashrc


RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64'>>/home/sparkuser/hadoop/etc/hadoop/hadoop-env.sh

RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64'>>~/.bashrc

RUN echo 'export HADOOP_MAPRED_HOME=$HADOOP_HOME'>>~/.bashrc
RUN echo 'export HADOOP_COMMON_HOME=$HADOOP_HOME'>>~/.bashrc
RUN echo 'export HADOOP_HDFS_HOME=$HADOOP_HOME'>>~/.bashrc
RUN echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop'>>~/.bashrc
RUN echo 'export HADOOP_YARN_HOME=$HADOOP_HOME'>>~/.bashrc
RUN echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native'>>~/.bashrc
RUN echo 'export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"'>>~/.bashrc

RUN echo 'export SPARK_DIST_CLASSPATH="$(hadoop classpath)"'>>~/.bashrc


CMD ["/bin/bash"]
