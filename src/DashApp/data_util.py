import sys
import os

sys.path.append('../python/')
import time
import logging
import pandas as pd
import numpy as np
import psycopg2


class DataUtil:
    """
    Class to query from database for app streaming.
    """

    def __init__(self):
        if not os.path.exists('./tmp'):
            os.makedirs('./tmp')
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/website.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        #self.postgres_config = helpers.parse_config(postgres_config_infile)
        self.cur = self.connectToDB()
        self.signal_schema = ['ecg']
        self.event_schema = ['id', 'signame', 'time', 'abnormal']

    def connectToDB(self):
        """
        :return: database cursor
        """
        cur = None
        try:
            conn = psycopg2.connect(host="ec2-34-220-61-87.us-west-2.compute.amazonaws.com",
                                    database="ecg",
                                    port="5432",
                                    user="ecg",
                                    password="pwd")
            cur = conn.cursor()
        except Exception as e:
            print(e)
        return cur

    def getECGSignal(self, ecg_id):
        """
        Queries signal_samples table to return the latest samples within the given interval.
        :param interval: time in seconds
        :return: dictionary of pandas dataframes containing latest samples within interval for each unique signame.
        """
        sqlcmd = "SELECT ecg\
                    FROM signal_samples WHERE id = {}\
                    AND ABNORMAL = 'TRUE';".format(ecg_id)
        self.cur.execute(sqlcmd)
        
        df = pd.DataFrame(self.cur.fetchall(), columns=self.signal_schema)

        return df
    
    def getLatestEvents(self, group_name, interval=10):
        """
        Queries signal_samples table to return the latest samples within the given interval.
        :param interval: time in seconds
        :return: dictionary of pandas dataframes containing latest samples within interval for each unique signame.
        """  
        sqlcmd = "SELECT id, signame, time, abnormal \
                    FROM signal_samples \
                    WHERE signame={}::VARCHAR\
                    AND abnormal = 'TRUE' \
                    ORDER BY time DESC \
                    LIMIT 50;".format(group_name)
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.event_schema)
        #get all the signal names unique in the dataframe above
        return df

    
    def getAllEvents(self, interval=60):
        """
        Queries signal_samples table to return the latest samples within the given interval.
        :param interval: time in seconds
        :return: dictionary of pandas dataframes containing latest samples within interval for each unique signame.
        """ 
        sqlcmd = "SELECT id, signame, time, abnormal \
                    FROM signal_samples WHERE time < (SELECT MAX(time) - interval '{} second' \
                    FROM signal_samples) \
                    ORDER BY time \
                    LIMIT 50;".format(interval)
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.event_schema)
        #get all the signal names unique in the dataframe above
        return df



if __name__ == '__main__':
    # Test the output of the queries.
    #postgres_config_infile = '../../.config/postgres.config'
    datautil = DataUtil()
    while True:
        print("inside while loop")
        name = "114"
        print(name)
        df = datautil.getAllEvents()
        #x=list(range(len(df.get_value(0,'ecg'))))
        #y_sig= df.get_value(0,'ecg')
        print(df)
        break