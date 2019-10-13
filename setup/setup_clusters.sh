peg up yml/kafkamaster.yml &
wait
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

############################################################

flink-cluster

peg up yml/flinkmaster.yml &
wait
peg up yml/flinkworkers.yml &

wait

peg fetch flink-cluster

peg install flink-cluster ssh
peg install flink-cluster aws
peg install flink-cluster environment
peg sshcmd-cluster flink-cluster "sudo apt-get install bc"
peg sshcmd-cluster flink-cluster "sudo apt-get update"
peg sshcmd-cluster website "sudo apt-get install maven"

peg install flink-cluster zookeeper
peg service flink-cluster zookeeper start

peg install flink-cluster flink
peg service flink-cluster flink start

#############################################################

peg up yml/website.yml &

wait

peg fetch website

peg install website ssh
peg install website aws
peg install website environment

peg sshcmd-cluster website "sudo apt-get install postgresql-server-dev-10"
peg sshcmd-cluster website "sudo apt-get install libpq-dev"
peg sshcmd-cluster website "pip install psycopg2 numpy pandas dash==0.28.1 dash-html-components==0.13.2 dash-core-components==0.30.2"

#############################################################

peg up yml/database.yml &

wait

peg fetch database

peg install database ssh
peg install database aws
peg install database environment