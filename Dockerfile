FROM apache/airflow:3.1.5

# 1. Chuyen sang quyen root de cai dat cac goi he thong (Java + C++ Compiler for SASL)
USER root

# Cai dat OpenJDK 17 va cac thu vien build can thiet cho SASL/Thrift
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
         gcc \
         g++ \
         pkg-config \
         git \
         libsasl2-dev \
         default-libmysqlclient-dev \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Thiet lap bien moi truong JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 2. Chuyen lai quyen airflow de cai dat thu vien Python
USER airflow

# Copy file requirements va cai dat
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
