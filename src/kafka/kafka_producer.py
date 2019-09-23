import sys
import os
import boto3
import time
from datetime import datetime
import pytz
from kafka.producer import KafkaProducer
import logging
sys.path.append('../python/')
import helpers

# TODO: Use kafka-connect to write directly from topic to postgresql database.

class Producer(object):
    """
    Class to ingest ecg signal data and send them to kafka topic through kafka producer.
    """

    def __init__(self, ip_addr, kafka_config_infile, s3bucket_config_infile):
        if not os.path.exists('./tmp'):
            os.makedirs('./tmp')
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/kafka_producer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')

        self.kafka_config = helpers.parse_config(kafka_config_infile)
        self.s3bucket_config = helpers.parse_config(s3bucket_config_infile)
        self.producer = KafkaProducer(bootstrap_servers=ip_addr)

    def produce_ecg_signal_msgs(self, file_key):
        """
        Produces messages and sends them to topic.
        """
        msg_cnt = 0

        while True:

            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.s3bucket_config['bucket'],
                                Key="%s_signals.txt" % file_key)
            for line in obj['Body'].iter_lines():
                message_info = None
                try:
                    linesplit = line.decode().split(',')
                    str_fmt = "{},{},{},{},{}"
                    message_info = str_fmt.format(file_key,
                                                  datetime.now(pytz.timezone('US/Eastern')),
                                                  linesplit[1],
                                                  linesplit[2],
                                                  linesplit[3]
                                                  )
                except Exception as e:
                    self.logger.error('fxn produce_ecg_signal_msgs error %s' % e)
                try:
                    msg = str.encode(message_info)
                except Exception as e:
                    msg = None
                    self.logger.debug('empty message %s'%e)
                if msg is not None:
                    self.producer.send(self.kafka_config['topic'], msg)
                    msg_cnt += 1
                print(message_info)
                time.sleep(0.001)


if __name__ == "__main__":
    # Initialize kafka producer with ip-address of broker and file to ingest from S3.
    print('kafka_producer called')
    args = sys.argv
    ip_addr = str(args[1])
    file_key = str(args[2])
    kafka_config_infile = '../../.config/kafka.config'
    s3bucket_config_infile = '../../.config/s3bucket.config'
    prod = Producer(ip_addr, kafka_config_infile, s3bucket_config_infile)
    prod.produce_ecg_signal_msgs(file_key)
