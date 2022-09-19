FROM 755674844232.dkr.ecr.us-east-1.amazonaws.com/spark/emr-6.8.0:latest
ARG SCALA_VERSION=2.12
ARG DELTA_VERSION=2.1.0

USER root
ADD https://repo1.maven.org/maven2/io/delta/delta-core_$SCALA_VERSION/$DELTA_VERSION/delta-core_$SCALA_VERSION-$DELTA_VERSION.jar ${SPARK_HOME}/jars/delta-core.jar
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/$DELTA_VERSION/delta-storage-$DELTA_VERSION.jar ${SPARK_HOME}/jars/delta-storage.jar
RUN chmod 644 ${SPARK_HOME}/jars/delta-*

# Use hadoop user and group 
USER hadoop:hadoop
