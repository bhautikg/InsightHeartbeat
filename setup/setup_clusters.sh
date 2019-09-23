peg up yml/kafkamaster.yml &
peg up yml/kafkaworkers.yml &

wait

peg fetch kafka-cluster

peg install kafka-cluster ssh
peg install kafka-cluster aws
peg install kafka-cluster environment
peg sshcmd-cluster kafka-cluster "sudo apt-get install bc"

peg install kafka-cluster zookeeper
peg service kafka-cluster zookeeper start

peg install kafka-cluster kafka
peg service kafka-cluster kafka start
