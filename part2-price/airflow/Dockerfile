FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY part1-ev/requirements.txt /tmp/de9-requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-apache-spark==4.11.3" \
    "pyspark==3.5.3" \
    -r /tmp/de9-requirements.txt \
    && pip check

USER root
RUN ln -s /home/airflow/.local/lib/python3.11/site-packages/pyspark /opt/spark \
    && mkdir -p /opt/spark/conf \
    && curl --fail --show-error --location --retry 3 \
       https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
       -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl --fail --show-error --location --retry 3 \
       https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
       -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
COPY --chown=airflow:root part1-ev/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PATH=/opt/spark/bin:/home/airflow/.local/bin:${PATH}
USER airflow
