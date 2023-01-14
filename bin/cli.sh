winpty docker run --rm -it \
                  -v //c/dev/kafka-connect-twitter-api-v2/build/config:/kafka-connect-source-twitter-api-v2/config \
                  -v //c/dev/kafka-connect-twitter-api-v2/offsets:/kafka-connect-source-twitter-api-v2/offsets \
                  --net=kafka-connect-twitter-api-v2_app-tier \
                  adamcc/kafka-connect-source-twitter-api-v2:0.1 \
                  bash