# Use the base Airflow image
FROM apache/airflow:2.10.2

USER root
# Copy requirements and install Python dependencies
COPY requirements.txt ./

RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
  # && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -o spark-3.4.0-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar xvf spark-3.4.0-bin-hadoop3.tgz && \
    mv spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

#Add Java to PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Add OCI CLI to PATH
ENV PATH="/root/bin:$PATH"

# Set SPARK_HOME & WEATHER API environment variable
ENV WEATHER_API_KEY="54ce3be99b6d93efb221eee5b5a8b52a"
ENV SPARK_HOME="/opt/spark"
ENV PATH="$PATH:$SPARK_HOME/bin"
#Set GCS as XCom backend
# ENV AIRFLOW__CORE__GCS_BUCKET_NAME=simo_gcs_airflow
# Switch back to airflow user
USER airflow
# Install Airflow Spark Provider
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --force-reinstall -r requirements.txt



