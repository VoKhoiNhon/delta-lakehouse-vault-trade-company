#############################
# Python Job
#############################
# Start with Python 3.11.9 (Bullseye-based) as the base image
FROM python:3.11.9-slim-bullseye as pyjob
WORKDIR /opt/itc
# Set environment variables for Spark installation
ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
# Install system dependencies
COPY ./requirements.txt /opt/itc/requirements.txt
COPY ./requirements_test.txt /opt/itc/requirements_test.txt
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -r /opt/itc/requirements.txt \
    && pip install -r /opt/itc/requirements_test.txt
# Download and install Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
COPY . /opt/itc
RUN sh /opt/itc/scripts/build-project.sh
RUN python3 scripts/init_package.py

#############################
# Jupyter Lab
#############################
FROM bitnami/spark:3.5.1 as jupyter-lab
WORKDIR /opt/itc
USER root
COPY requirements.txt /opt/itc/requirements.txt
RUN apt update -y \
    && pip install jupyterlab==4.2.0 \
    && pip install -r requirements.txt
ENV PYTHONPATH="${PYTHONPATH}:/opt/itc"
COPY . /opt/itc
RUN sh /opt/itc/scripts/build-project.sh
RUN python3 scripts/init_package.py
EXPOSE 8888

#############################
# Custom Bitnami/Spark
#############################
FROM bitnami/spark:3.5.1 as bitnami-spark-custom
USER root
WORKDIR /opt/itc
COPY ./requirements.txt /opt/itc/requirements.txt
COPY ./requirements_test.txt /opt/itc/requirements_test.txt
RUN apt update -y \
    && pip install -r /opt/itc/requirements.txt \
    && pip install -r /opt/itc/requirements_test.txt
COPY . /opt/itc
RUN sh /opt/itc/scripts/build-project.sh
RUN python3 scripts/init_package.py

#############################
# HIVE METASTORE
#############################
FROM openjdk:8-jre-slim-buster as hive-metastore
WORKDIR /opt
ENV HADOOP_VERSION=3.2.0
ENV METASTORE_VERSION=3.0.0
RUN apt-get update && apt-get install -y netcat curl
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin
RUN curl -L https://downloads.apache.org/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zxf - && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf - && \
    curl -L https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.19.tar.gz | tar zxf - && \
    curl -L --output postgresql-42.4.0.jar https://jdbc.postgresql.org/download/postgresql-42.4.0.jar && \
    curl -L --output hive-exec-${METASTORE_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${METASTORE_VERSION}/hive-exec-${METASTORE_VERSION}.jar && \
    cp mysql-connector-java-8.0.19/mysql-connector-java-8.0.19.jar ${HIVE_HOME}/lib/ && \
    cp postgresql-42.4.0.jar ${HIVE_HOME}/lib/ && \
    cp hive-exec-${METASTORE_VERSION}.jar ${HIVE_HOME}/lib/ && \
    rm -rf mysql-connector-java-8.0.19 && \
    rm -rf postgresql-42.4.0.jar && \
    rm -rf hive-exec-${METASTORE_VERSION}.jar
COPY scripts/hive-metastore-entrypoint.sh /hive-metastore-entrypoint.sh
RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chown hive:hive /hive-metastore-entrypoint.sh && chmod +x /hive-metastore-entrypoint.sh
USER hive
EXPOSE 9083
WORKDIR ${HIVE_HOME}
ENTRYPOINT ["sh", "-c", "/hive-metastore-entrypoint.sh"]

####################
# APACHE SUPERSET
####################
FROM apache/superset:4.1.1 as superset-custom
USER root
RUN apt update -y \
    && apt install -y gcc \
    && pip install psycopg2
USER superset
