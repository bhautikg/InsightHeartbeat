/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic ecg-topic --partitions 4 --replication-factor 2 --config retention.ms=600000