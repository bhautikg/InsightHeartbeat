IP_ADDR="$(hostname)"
DEPS="../../python/helpers.py,../spark_helpers.py,../spark_consumer.py"
DEPS_ARR=($DEPS)

/usr/local/spark/bin/spark-submit --master spark://$IP_ADDR:7077 --num-executors 3 --packages org.apache.spark/spark-streaming-kafka-0-8_2.11/2.3.1 --py-files $DEPS  main_ecg_consumer.py
