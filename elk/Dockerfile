FROM jupyter/pyspark-notebook:latest

# Install iputils-ping for ping functionality
USER root
RUN apt-get update -y && apt-get install -y iputils-ping && apt-get install -y telnet

# Pre-download the Spark Kafka package and other dependencies
USER jovyan
#ENV SPARK_HOME=/usr/local/spark
#RUN ${SPARK_HOME}/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0



RUN pip install kafka-python
# RUN pip install "tensorflow<2.11"
# RUN conda install -c conda-forge cudatoolkit=11.2 cudnn=8.1.0

# To run the Python files, use spark-submit to include necessary packages
# docker exec -it -u root pyspark bash
# spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1   --jars kafka-clients-3.5.0.jar   --driver-class-path kafka-clients-3.5.0.jar   --conf "spark.executor.extraClassPath=kafka-clients-3.5.0.jar" keep_reading.py

# If tensorflow is not isntalled with the dockerfile, run this in the cmd
# docker run -it tensorflow/tensorflow
