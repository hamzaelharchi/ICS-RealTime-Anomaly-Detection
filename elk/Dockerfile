FROM jupyter/pyspark-notebook:latest

# Install iputils-ping for ping functionality
USER root
RUN apt-get update -y && apt-get install -y iputils-ping && apt-get install -y telnet

# Pre-download the Spark Kafka package and other dependencies
USER jovyan
#ENV SPARK_HOME=/usr/local/spark
#RUN ${SPARK_HOME}/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0



RUN pip install kafka-python
#RUN spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1   --jars kafka-clients-3.5.0.jar   --driver-class-path kafka-clients-3.5.0.jar   --conf "spark.executor.extraClassPath=kafka-clients-3.5.0.jar" work/keep_reading.py

#docker exec -it -u root pyspark bash
