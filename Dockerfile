FROM flink:1.16.2-scala_2.12-java11

ENV FLINK_VERSION=1.16.2
ENV MAVEN_ROOT_URL=https://repo.maven.apache.org/maven2

# Install Kafka connector
RUN wget -P /opt/flink/lib/ ${MAVEN_ROOT_URL}/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar;

WORKDIR /opt/flink
