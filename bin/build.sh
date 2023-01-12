./gradlew clean build -xtest

docker image rm adamcc/kafka-connect-source-twitter-api-v2:0.1

docker build . -t adamcc/kafka-connect-source-twitter-api-v2:0.1
