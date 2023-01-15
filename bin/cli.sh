PWD=$(pwd)
winpty docker run --rm -it \
                  -v /${PWD}/build/config:/kafka-connect-source-twitter-api-v2/config \
                  -v /${PWD}/offsets:/kafka-connect-source-twitter-api-v2/offsets \
                  --net=kafka-connect-twitter-api-v2_app-tier \
                  adamcc/kafka-connect-source-twitter-api-v2:0.2 \
                  bash