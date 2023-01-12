FROM docker.io/bitnami/kafka:3.2.3

WORKDIR /kafka-connect-source-twitter-api-v2
COPY config config
COPY build/libs libs
COPY bin/start_connector.sh bin/start_connector.sh
COPY bin/consume_topic.sh bin/consume_topic.sh
COPY bin/create_topic.sh bin/create_topic.sh

COPY sdk.properties .

VOLUME /kafka-connect-source-twitter-api-v2/config
VOLUME /kafka-connect-source-twitter-api-v2/offsets

USER root
RUN echo log4j.logger.org.reflections.Reflections=ERROR >> /opt/bitnami/kafka/config/connect-log4j.properties
USER 1001

ENV KAFKA_LOG4J_OPTS "-Dlog4j.configuration=${KAFKA_HOME}/config/connect-log4j.properties"

CMD bin/start_connector.sh
