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
        self.signal_schema = ['id', 'signame', 'time', 'ecg', 'abnormal']

    def connectToDB(self):
        """
        :return: database cursor
        """
        cur = None
        try:
            print("Inside connect to db")
            conn = psycopg2.connect(host="ec2-34-220-61-87.us-west-2.compute.amazonaws.com",
                                    database="ecg",
                                    port="5432",
                                    user="ecg",
                                    password="pwd")
            cur = conn.cursor()
        except Exception as e:
            print(e)
        return cur

    def getLastestECGSamples(self, interval=10):
        """
        Queries signal_samples table to return the latest samples within the given interval.
        :param interval: time in seconds
        :return: dictionary of pandas dataframes containing latest samples within interval for each unique signame.
        """
        sqlcmd = "SELECT id, signame, time, ecg, abnormal \
                    FROM signal_samples WHERE time > (SELECT MAX(time) - interval '{} second' \
                    FROM signal_samples) \
                    AND ABNORMAL = TRUE \
                    ORDER BY signame;".format(interval)
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.signal_schema)
        #get all the signal names unique in the dataframe above
        signames = df[self.signal_schema[1]].unique()
        #Creates an dictionary for each signal name as key, and a empty dataframe obj as valye
        signals_dict = {elem: pd.DataFrame for elem in signames}

        #Populates the dictionary for each key, with the the dataframe obtained from postgres for that key
        for key in signals_dict.keys():
            #signals_dict[key] = df[:][df.signame == key]
            #print(df['signame'] == key)
            signals_dict[key] = df.get_value(0, 'ecg')
            print(signals_dict[key])

            #signals_dict[key].sort_values('time', inplace=True)
        
        return signals_dict.keys(), signals_dict


if __name__ == '__main__':
    # Test the output of the queries.
    #postgres_config_infile = '../../.config/postgres.config'
    datautil = DataUtil()
    while True:
        print("inside while loop")
        keys_ecg, signals_dict = datautil.getLastestECGSamples()

        for key in keys_ecg:
            print('ecg samples: ', key, len(signals_dict[key]))
        time.sleep(1)
        break