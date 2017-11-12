import urllib.request, json
import pandas as pd
import datetime
import sqlite3
import os
import warnings
warnings.simplefilter("ignore")

class bitcoin(object):
    '''
    Fetch bitcoin data from bitstamp's website, which gets updated regularly.
    Append.
    Save.
    Load.
    '''
    def __init__(self):
        self.database_path = "C:/Users/jkim118/Documents/bitcoin/data/"
        self.time_now = str(datetime.datetime.now())
        self.compiled_data = self._load_compiled_data()
        self.today_data = self._load_today_data()
        self.data = self._compile_data()

    def _load_today_data(self):
        '''
        Load bitcoin data as of right now and check for duplicates.
        '''
        bitcoin_url = "https://www.bitstamp.net/api/ticker/"
        def timestamp_to_date(timestamp):
            date = datetime.datetime.fromtimestamp(int(timestamp))\
                                                  .strftime('%Y-%m-%d %H:%M:%S')
            return date
        with urllib.request.urlopen(bitcoin_url) as url:
            data = pd.DataFrame(json.loads(url.read().decode()),
                                index=[len(self.compiled_data)])
            data['date'] = ""
            data['date'][len(self.compiled_data)] = timestamp_to_date(\
                                data['timestamp'][len(self.compiled_data)])
            print("bitcoin data updated on {}".format(self.time_now))
        if self._check_duplicates(data):
            return "see the last row of data"
        else:
            return data

    def _compile_data(self):
        '''
        Compile the existing and new bitcoin data together.
        '''
        data = self._update_database()
        print("bitcoin data from 11/11/17 to {}".format(self.time_now))
        return data

    def _load_compiled_data(self):
        '''
        Load compiled bitcoin data.
        '''
        conn = sqlite3.connect(os.path.join(self.database_path, "bitcoin.db"))
        compiled_data = pd.read_sql_query("SELECT * from data;", conn)
        print("compiled data loaded")
        return compiled_data

    def _check_duplicates(self, data):
        '''
        Arguments:
        data : pd.DataFrame
               bitcoin data as of now
        '''
        return self.compiled_data.isin(data).any().all()

    def _update_database(self):
        if isinstance(self.today_data, str):
            data = self.compiled_data
        else:
            data = pd.concat([self.compiled_data, self.today_data])
        conn = sqlite3.connect(os.path.join(self.database_path, "bitcoin.db"))
        data.to_sql("data", conn, if_exists="replace",
                                          index = False)
        conn.close()
        data.to_csv(os.path.join(self.database_path, "bitcoin.csv"))
        print("Bitcoin data tables were last updated on {}"\
             .format(self.time_now))
        return data
