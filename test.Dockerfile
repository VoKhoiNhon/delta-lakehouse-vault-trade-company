FROM bitnami/spark:3.5.3
USER root

RUN apt-get update && apt-get install -y curl

ENV SHARED_WORKSPACE=/opt/workspace
ENV SPARK_VERSION=3.3
RUN mkdir -p ${SHARED_WORKSPACE}
RUN mkdir -p /opt/jars

COPY spark-test.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY requirements.txt ${SHARED_WORKSPACE}/requirements.txt
COPY requirements_test.txt ${SHARED_WORKSPACE}/requirements_test.txt
# COPY jars/deequ-2.0.3-spark-3.3.jar ${SHARED_WORKSPACE}/jars/deequ-2.0.3-spark-3.3.jar
RUN curl -o /opt/jars/deequ-2.0.3-spark-3.3.jar \
  https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.3-spark-3.3/deequ-2.0.3-spark-3.3.jar

RUN curl -o /opt/jars/hadoop-aws-3.3.1.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar


WORKDIR ${SHARED_WORKSPACE}
RUN pip3 install -r requirements.txt
RUN pip3 install -r requirements_test.txt


RUN (echo "from pyspark.sql import SparkSession"; echo "try: SparkSession.builder.config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.1.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.2,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.1').getOrCreate()";  echo "except: pass") >> init_parkages.py
RUN ${SPARK_HOME}/bin/spark-submit init_parkages.py