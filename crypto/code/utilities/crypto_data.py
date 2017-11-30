import urllib.request, json
import pandas as pd
import datetime
import sqlite3
import os
import warnings
warnings.simplefilter("ignore")

class crypto(object):
    '''
    Fetch crypto-currency data from bitstamp's website,
    which gets updated regularly.
    Append.
    Save.
    Load.
    '''
    def __init__(self, crypto_kind):
        self.database_path = "/Users/junkim/Documents/crypto/crypto/data/"
        self.crypto_kind = crypto_kind.lower()
        self.time_now = str(datetime.datetime.now())
        self.kind,self.bitstamp_url = self._get_bitstamp_url()
        self._initiate_database()
        self.compiled_data = self._load_compiled_data()
        self.today_data = self._load_today_data()
        self.data = self._compile_data()

    def _get_bitstamp_url(self):
        kinds = {
                 "bitcoin":"btcusd",
                 "bit":"btcusd",
                 "btc":"btcusd",
                 "bit coin":"btcusd",
                 "ripple":"xrpusd",
                 "ripple coin":"xrpusd",
                 "xrp":"xrpusd",
                 "lite":"ltcusd",
                 "lite coin":"ltcusd",
                 "ether":"ethusd",
                 "ethereum":"ethusd",
                 "eth":"ethusd"
                 }
        kind = kinds[self.crypto_kind]
        bitstamp_url = "https://www.bitstamp.net/api/v2/ticker/{currency}/".\
                        format(currency=kind)
        return kind,bitstamp_url

    def _load_data(self, index):
        def timestamp_to_date(timestamp):
            date = datetime.datetime.fromtimestamp(int(timestamp))\
                                                  .strftime('%Y-%m-%d %H:%M:%S')
            return date
        with urllib.request.urlopen(self.bitstamp_url) as url:
            data = pd.DataFrame(json.loads(url.read().decode()),
                                index=[index])
            data['date'] = ""
            data['date'][index] = timestamp_to_date(\
                                data['timestamp'][index])
        return data

    def _load_today_data(self):
        '''
        Load crypto data as of right now and check for duplicates.
        '''
        data = self._load_data(len(self.compiled_data))
        print("{} data updated on {}".format(self.kind,self.time_now))
        if self._check_duplicates(data):
            return "see the last row of data"
        else:
            return data

    def _compile_data(self):
        '''
        Compile the existing and new crpto data together.
        '''
        data = self._update_database()
        print("{} data from 11/11/17 to {}".format(self.kind, self.time_now))
        return data

    def _load_compiled_data(self):
        '''
        Load compiled crypto data.
        '''
        conn = sqlite3.connect(os.path.join(self.database_path, "data.db"))
        compiled_data = pd.read_sql_query("SELECT * from {};".\
                                            format(self.kind), conn)
        print("compiled data loaded")
        return compiled_data

    def _check_duplicates(self, data):
        '''
        Arguments:
        data : pd.DataFrame
               crypto data as of now
        '''
        return self.compiled_data.isin(data).any().all()

    def _save_data(self,data):
        conn = sqlite3.connect(os.path.join(self.database_path, "data.db"))
        data.to_sql("{}".format(self.kind), conn, if_exists="replace",
                                          index = False)
        conn.close()
        data.to_csv(os.path.join(self.database_path, "{}.csv".\
                                                        format(self.kind)))

    def _initiate_database(self):
        '''
        Purpose:
        Initiate the database for each table if the table does not exist.
        '''
        try:
            self._load_compiled_data()
        except:
            data = self._load_data(0)
            self._save_data(data)
            print("{} table was set up".format(self.kind))

    def _update_database(self):
        if isinstance(self.today_data, str):
            data = self.compiled_data
        else:
            data = pd.concat([self.compiled_data, self.today_data])
        data = data.drop_duplicates('timestamp').reset_index(drop=True)
        self._save_data(data)
        print("{} data table was last updated on {}"\
             .format(self.kind,self.time_now))
        return data
