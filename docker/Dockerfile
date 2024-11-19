FROM bitnami/spark:3.5

# Install Python and dependencies for PySpark and Jupyter Lab
USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install pyspark jupyterlab

# Set up the Spark environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Set up Jupyter Lab to run on container start
CMD ["/bin/bash", "-c", "jupyter lab --ip=0.0.0.0 --allow-root --NotebookApp.token='' --port=8888"]
