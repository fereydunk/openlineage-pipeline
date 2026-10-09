# Self-managed Kafka Connect worker with the Confluent JDBC source connector
# and the IBM Db2 JDBC driver, so it can poll Db2 and produce to Confluent Cloud.
FROM confluentinc/cp-kafka-connect:7.7.1

# JDBC source/sink connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.0

# IBM Db2 JDBC driver (jcc) into the JDBC connector's lib so it's on the classpath
ARG DB2_JCC_VERSION=11.5.9.0
RUN curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/jcc-${DB2_JCC_VERSION}.jar \
    https://repo1.maven.org/maven2/com/ibm/db2/jcc/${DB2_JCC_VERSION}/jcc-${DB2_JCC_VERSION}.jar
