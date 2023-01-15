docker image rm adamcc/kafka-connect-source-twitter-api-v2:0.2
./gradlew clean build && \
docker build . -t adamcc/kafka-connect-source-twitter-api-v2:0.2
