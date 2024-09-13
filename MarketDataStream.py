import os.path

from binance.um_futures import UMFutures
from FileIO import FileIO
import pandas as pd
import numpy as np

#Helper functions that can be static rather than implemented as method
def convert_to_df(data):
    data_df=pd.DataFrame(np.asarray(data,dtype=np.float64),columns=["Open Time","Open","High","Low","Close","Volume","Close Time","Quote Asset Volume","Number of Trades","Taker Buy Volume","Taker Buy Quote Asset Volume","Ignore"])
    data_df['Open Time'] = pd.to_datetime(data_df['Open Time'], unit='ms')
    data_df['Close Time'] = pd.to_datetime(data_df["Close Time"],unit='ms').dt.ceil("min")
    return data_df.drop("Ignore",axis=1)


def datetime_to_unix(dates):
    return (dates - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')


def returnPairFile(pair_in, interval_in):
    return f"MarketData/{pair_in}/{pair_in}_{interval_in}.csv"


def calculateTimeDifferenceAsInterval(pair_in, interval_in):
    interval_in_as_int = int(interval_in[:-1])
    file = pd.read_csv(returnPairFile(pair_in, interval_in))
    last_close_timestamp = pd.to_datetime(file.iloc[-1]["Close Time"])
    timestamp_now = pd.Timestamp.now() - pd.Timedelta(hours=3)
    time_difference = timestamp_now - last_close_timestamp
    time_diff_in_interval = (time_difference.total_seconds() / 60) / interval_in_as_int
    return np.ceil(time_diff_in_interval), last_close_timestamp


def getWhitelistPairs():
    whitelist_txt = open("whitelist.txt","r")
    pair_list = []
    for pair in whitelist_txt:
        pair_list.append(pair[:-1])
    return pair_list


class MarketDataFetcher:
    def __init__(self,api_key=0,api_secret=0 ):
        self.base_endpoint = "https://fapi.binance.com"
        self.api_key = api_key
        self.api_secret = api_secret

        self.FileIO = FileIO()
        self.intervals = self.FileIO.intervals
        self.market_data_directory = self.FileIO.marketDataDirectory
        self.whitelist = getWhitelistPairs()
        if api_key == 0 and api_secret == 0:
            self.client = UMFutures("iPQMe46exUy10KiBaBahQT7ow1uzb9jaxlKj19Bg5BI8JEwJL5bw9LCvJtfKuVbP",
                                    "J4FGJQdGPpNGm8jpnj3IuFuDJuAe7FIa3cM51L4z662UYux3qD0ByLJ0bfIltZvK")


    def get_pair_data(self,pair_in,interval_in,**kwargs):
        return convert_to_df(self.client.continuous_klines(pair_in,contractType="PERPETUAL", interval=interval_in,limit=1000,**kwargs))

    def getPairAllPastData(self,pair_in, interval_in):

        count = 100
        df = self.get_pair_data(pair_in, interval_in)
        for i in range(count):
            begin_time_as_unix = datetime_to_unix(df["Open Time"].iloc[0])
            df_before = self.get_pair_data(pair_in,interval_in, endTime=begin_time_as_unix)
            df = pd.concat([df_before,df],axis = 0)

        df = df.drop_duplicates(keep="last",subset = "Open Time")
        df.to_csv(os.path.join("MarketData",pair_in,f"{pair_in}_{interval_in}.csv"),index = False)

    def getWhitelistPairsPastData(self):
        for interval in self.intervals:
            for pair in self.whitelist:
                if not os.path.exists(f"MarketData/{pair}/{pair}_{interval}.csv"):
                    self.getPairAllPastData(pair, interval)


    def updateWhitelistData(self,pair_in,interval_in):
        interval_as_int = int(interval_in[:-1])
        interval_mode = interval_in[-1]
        time_diff,begin_time = calculateTimeDifferenceAsInterval(pair_in,interval_in)
        data = pd.read_csv(returnPairFile(pair_in, interval_in))

        for i in range(int(np.ceil(time_diff/1000))):
            data = pd.concat([data, self.get_pair_data(pair_in, interval_in, startTime=datetime_to_unix(begin_time))], axis=0)
            begin_time += pd.Timedelta(minutes=interval_as_int)*1000 if interval_mode == 'm' else pd.Timedelta(hours=interval_as_int)

        data.to_csv(returnPairFile(pair_in, interval_in),index = False)

    def updateAllWhitelistData(self):
        for pair in self.whitelist:
            for interval in self.intervals:
                self.updateWhitelistData(pair,interval)
                print(f"{pair}:{interval} last updated in {pd.Timestamp.now()}")

