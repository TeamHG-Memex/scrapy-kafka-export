# /bin/bash

set -eu

MIRROR=http://www-us.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz
test -f kafka.tgz || wget $MIRROR -O kafka.tgz
# Unpack the kafka over the top of any existing installation
echo "Unpacking Kafka installation file"
mkdir -p kafka && tar xzf kafka.tgz -C kafka --strip-components 1
# Start the zookeeper running in the background.
# By default the zookeeper listens on localhost:2181
echo "Starting Zookeeper"
kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties
echo -e "\ndelete.topic.enable=true" >> kafka/config/server.properties
# Start the kafka server running in the background.
# By default the kafka listens on localhost:9092
echo "Starting Kafka"
kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties
sleep 10