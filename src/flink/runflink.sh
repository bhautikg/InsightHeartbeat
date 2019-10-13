cd ~/InsightHeartbeat/src/Flink
mvn clean package
cd /usr/local/flink
./bin/flink run ~/InsightHeartbeat/src/Flink/target/flink-process-v1.jar