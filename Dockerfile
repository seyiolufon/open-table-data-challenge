FROM bitnami/spark:3.5.5

WORKDIR /opt/spark-app


COPY app/ ./app/

COPY requirements.txt .
USER root

RUN pip install --no-cache-dir kafka-python pyspark

RUN apt-get update && apt-get install -y python3-pip curl && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/*

USER 1001

CMD ["sleep", "infinity"]