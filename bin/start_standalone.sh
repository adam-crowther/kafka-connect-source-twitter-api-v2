export CLASSPATH="$(find libs/ -type f -name '*.jar' | tr '\n' ':'  | sed 's/:$//')"
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file://$(pwd)/config/log4j.properties"

# This assumes the kafka server is running in a docker container called "fast-data-dev" on port 9092
# and advertising listener 127.0.0.1:9092.  Socat runs a port forwarding service of port 9092 to the
# fast-data-dev container.
socat tcp-listen:9092,reuseaddr,fork tcp:fast-data-dev:9092 &

connect-standalone config/worker.properties config/TwitterApiV2ConnectorSource.properties
