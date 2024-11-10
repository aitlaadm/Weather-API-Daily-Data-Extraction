# Use the base Airflow image
FROM apache/airflow:2.10.2

# Copy requirements and install Python dependencies
COPY requirements.txt ./
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download and install Spark
# RUN curl -o spark-3.4.0-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
#     tar xvf spark-3.4.0-bin-hadoop3.tgz && \
#     mv spark-3.4.0-bin-hadoop3 /opt/spark && \
#     rm spark-3.4.0-bin-hadoop3.tgz

# Set SPARK_HOME environment variable
# ENV SPARK_HOME="/opt/spark"
# ENV PATH="$PATH:$SPARK_HOME/bin"
USER airflow
# Install Airflow Spark Provider
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Switch back to airflow user

