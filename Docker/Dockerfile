FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r requirements.txt
RUN mkdir -p /opt/spark-jars && \
    curl -o /opt/spark-jars/spark-sql-kafka-0-10_2.13-4.2.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar && \
    curl -o /opt/spark-jars/kafka-clients-3.9.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    curl -o /opt/spark-jars/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
    ;

COPY . .

CMD ["tail", "-f", "/dev/null"]