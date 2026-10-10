FROM apache/spark:3.5.3-python3

USER root
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app
RUN pip3 install --no-cache-dir kafka-python==2.2.0 elasticsearch==8.15.1
COPY spark_jobs ./spark_jobs

# The Kafka connector is fetched at start-up; it needs outbound access to Maven Central.
CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
     "spark_jobs/detector.py"]
