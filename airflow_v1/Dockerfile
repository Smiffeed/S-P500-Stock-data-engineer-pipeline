FROM apache/airflow:3.1.8

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir \
    kaggle \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-google \
    google-cloud-storage \
    pyspark==4.0.1 \
    pytest
