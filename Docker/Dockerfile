FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

COPY Docker/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r requirements.txt
RUN mkdir -p /opt/spark-jars && \
    curl --retry 10 --retry-delay 5 --max-time 120 -o /opt/spark-jars/spark-sql-kafka-0-10_2.13-4.2.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar && \
    curl --retry 10 --retry-delay 5 --max-time 120 -o /opt/spark-jars/kafka-clients-3.9.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    curl --retry 10 --retry-delay 5 --max-time 120 -o /opt/spark-jars/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar && \
    curl --retry 10 --retry-delay 5 --max-time 120 -o /opt/spark-jars/commons-pool2-2.13.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar \
    ;

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app/app.py", "--server.address=0.0.0.0", "--server.port=8501"]