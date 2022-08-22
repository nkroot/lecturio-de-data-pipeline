FROM base

# -- Layer: Image Metadata

ARG build_date

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - JupyterLab Image"
LABEL org.label-schema.description="JupyterLab image"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: Notebooks and data

# ADD workspace/ ${SHARED_WORKSPACE}/

# -- Layer: JupyterLab + Python kernel for PySpark

ARG spark_version
ARG jupyterlab_version

# -- Runtime

ENV MASTER_CONTAINER_NAME=spark-master
ENV SPARK_EXECUTOR_MEMORY=1G
ENV SPARK_EXECUTOR_CORES=3

# Install libraries

WORKDIR ${SHARED_WORKSPACE}/app

COPY ./requirements.txt ./


RUN apt-get update -y && \
    pip3 install wget==3.2 jupyterlab==${jupyterlab_version} && \
    pip3 install -r requirements.txt && \
    spark-submit main.py
    

# Copy examples python files into container


# RUN spark-submit main.py

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
