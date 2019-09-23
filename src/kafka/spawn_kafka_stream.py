import os
import sys
sys.path.append('../python')
import boto3
import helpers

# TODO: Fix brokers unavailable issue now switched over the confluent-kafka.

session = 'k1'
kafka_config = helpers.parse_config('../../.config/kafka.config')
s3bucket_config = helpers.parse_config('../../.config/s3bucket.config')
ipaddr = kafka_config['ip-addr'].split(',')
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=s3bucket_config['bucket'],
                    Key="RECORDS_abridged.txt")
records = obj['Body'].read().decode('utf-8').split('\n')

records_per_node = int(round(len(records)/len(ipaddr)))
print(records_per_node)

#Open x number of file threads on y nodes. Visualized using tmux.
os.system('tmux kill-session -t %s'%session)
os.system('tmux new-session -s %s -n bash -d'%session)
for i in range(len(ipaddr)):
    start = i*records_per_node
    if i == len(ipaddr):
        stop = len(records) -1
    else:
        stop = (i+1) * records_per_node
    ip = ipaddr[i]
    records_interval = records[start:stop]
    os.system('echo %s'%ip)
    for j in range(len(records_interval)):
        session_num = i*len(records_interval)+j + 1
        record = records_interval[j]
        os.system('echo %s' % record)
        os.system('tmux new-window -t %d'%(session_num))
        os.system("tmux send-keys -t %s:%d 'python kafka_producer.py %s %s' C-m"%(session, session_num, ip, record))
