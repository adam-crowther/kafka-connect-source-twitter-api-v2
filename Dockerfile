FROM docker.io/landoop/fast-data-dev:latest
WORKDIR /kafka-connect-source-twitter-api-v2
COPY config config
COPY build/libs libs
COPY bin/start_standalone.sh bin/start_standalone.sh
COPY bin/consume_topic.sh bin/consume_topic.sh
COPY bin/create_topic.sh bin/create_topic.sh

COPY sdk.properties .

VOLUME /kafka-connect-source-twitter-api-v2/config
VOLUME /kafka-connect-source-twitter-api-v2/offsets

RUN apt update
RUN apt install -y socat

ENV KAFKA_LOG4J_OPTS "-Dlog4j.configuration=${KAFKA_HOME}/config/connect-log4j.properties"

CMD bin/start_standalone.sh
