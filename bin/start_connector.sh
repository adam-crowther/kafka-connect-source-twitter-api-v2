export CLASSPATH="$(find libs/ -type f -name '*.jar' | tr '\n' ':')"
connect-standalone.sh config/worker.properties config/TwitterApiV2ConnectorSource.properties
