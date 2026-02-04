FROM quay.io/astronomer/astro-runtime:12.6.0

# Install OpenJDK-17 (Required for PySpark)
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER astro