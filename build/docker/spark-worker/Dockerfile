ARG spark_version
FROM spark-base:${spark_version}


# -- Layer: Image Metadata

ARG build_date

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Spark Worker Image"
LABEL org.label-schema.description="Spark worker image"
# LABEL org.label-schema.url="https://github.com/andre-marcos-perez/spark-cluster-on-docker"
LABEL org.label-schema.schema-version="1.0"

# -- Runtime
ARG spark_worker_web_ui=8081

EXPOSE ${spark_worker_web_ui}

CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> logs/spark-worker.out