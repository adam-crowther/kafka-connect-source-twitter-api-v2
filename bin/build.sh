docker image rm adamcc/kafka-connect-source-twitter-api-v2:0.1
./gradlew clean build -xtest && \
docker build . -t adamcc/kafka-connect-source-twitter-api-v2:0.1
