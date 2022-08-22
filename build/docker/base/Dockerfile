ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:${debian_buster_image_tag}

# -- Layer: Image Metadata

ARG build_date

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Cluster Base Image"
LABEL org.label-schema.description="Cluster base image shipped with Python and a shared workspace folder"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: OS + Python 

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace}/data && \
    mkdir -p ${shared_workspace}/app && \
    mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y curl python3 && \ 
    apt-get install -y python3-pip python3-dev && \
    pip3 install --upgrade pip && \    
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV PATH=${PATH}
ENV SHARED_WORKSPACE=${shared_workspace}

COPY /.. ${SHARED_WORKSPACE}/app

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]