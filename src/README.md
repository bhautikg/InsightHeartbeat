This folder contains the source code for the application split by technology.

Clone this repo onto the master node of each cluster. Start the technologies in the following order.



### Downloading Data to S3

1. Spin up an EC2 machine, or do it on your local
2. pip install boto3, pip install wfdb, pip install numpy
3. Download the following RECORDS file as text, into the appropriate folder "./data/mitdb" link: https://archive.physionet.org/physiobank/database/mitdb/RECORDS
4. cd into the correct folder: ./Python
5. run the python script: python physionetdownloadscript.py
6. (if you have trouble, its either with python3, or python, and using pip or pip3 correctly, I used pip3)
7. Do the following in command line. 
8. sudo apt install awscli
9. pip3 install awscli --upgrade     --user
10. aws configure (on command line)
11. aws s3 sync </Directory with the data you want to sync/>    s3://</bucket name/>
12. i.e "aws s3 sync ./data/ s3://testsmalldata/"

### Database

1. Follow the instructions in ../setup/setup_db.txt 

### Kafka Cluster
1. cd into the correct folder: ./Kafka
2. Make sure to change the URLs, IP addresses, and S3 database names to appropriate values for your project in kafka_producer.py, and spawn_kafka_stream.py
3. Make the topic for the kafka brokers to send messages to: bash maketopics.sh
4. Start the kafka producers: python spawn_kafka_stream.py

### Flink Cluster
1. cd into the correct folder: ./Flink
2. Start the spark consumers: bash runflink.sh

### Website
1. cd into the correct folder: ./DashApp
2. Start the dash application: sudo python InsightDash.py
