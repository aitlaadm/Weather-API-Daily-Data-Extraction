# Use the base Airflow image
FROM apache/airflow:2.10.2

# Copy requirements and install Python dependencies
COPY requirements.txt ./

# Install Java (needed for Spark)
# USER root
# RUN apt-get update && \
#     apt-get install -y software-properties-common curl && \
#     add-apt-repository -y ppa:openjdk-r/ppa && \
#     apt-get update && \
#     apt-get install -y openjdk-11-jdk && \
#     rm -rf /var/lib/apt/lists/*
USER root

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/opt/bitnami"

# # Download and install Spark
# RUN curl -o spark-3.4.0-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
#     tar xvf spark-3.4.0-bin-hadoop3.tgz && \
#     mv spark-3.4.0-bin-hadoop3 /opt/spark && \
#     rm spark-3.4.0-bin-hadoop3.tgz

# # Set SPARK_HOME environment variable
# ENV SPARK_HOME="/opt/spark"
# ENV PATH="$PATH:$SPARK_HOME/bin"
USER airflow
# Install Airflow Spark Provider
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Switch back to airflow user

