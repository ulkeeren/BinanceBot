import os.path

from binance.um_futures import UMFutures
from FileIO import FileIO
import pandas as pd
import numpy as np
class MarketDataFetcher():
    def __init__(self,api_key=0,api_secret=0 ):
        self.base_endpoint = "https://fapi.binance.com"
        self.api_key = api_key
        self.api_secret = api_secret
        self.FileIO = FileIO()
        self.market_data_directory = self.FileIO.marketDataDirectory
        if api_key == 0 and api_secret == 0:
            self.client = UMFutures("iPQMe46exUy10KiBaBahQT7ow1uzb9jaxlKj19Bg5BI8JEwJL5bw9LCvJtfKuVbP",
                                    "J4FGJQdGPpNGm8jpnj3IuFuDJuAe7FIa3cM51L4z662UYux3qD0ByLJ0bfIltZvK")

    def get_pair_data(self,pair_in,interval_in,**kwargs):
        return self.convert_to_df(self.client.continuous_klines(pair_in,contractType="PERPETUAL", interval=interval_in,limit=1000,**kwargs))

    def convert_to_df(self, data):
        data_df=pd.DataFrame(np.asarray(data,dtype=np.float64),columns=["Open Time","Open","High","Low","Close","Volume","Close Time","Quote Asset Volume","Number of Trades","Taker Buy Volume","Taker Buy Quote Asset Volume","Ignore"])
        data_df['Open Time'] = pd.to_datetime(data_df['Open Time'], unit='ms')
        data_df['Close Time'] = pd.to_datetime(data_df["Close Time"],unit='ms').dt.ceil("min")
        return data_df.drop("Ignore",axis=1)

    def getPairAllPastData(self,pair_in, interval_in):
        count = 100
        df = self.get_pair_data(pair_in, interval_in)
        for i in range(count):
            begin_time_as_unix = self.datetime_to_unix(df["Open Time"].iloc[0])
            df_before = self.get_pair_data(pair_in,interval_in, endTime=begin_time_as_unix)
            df = pd.concat([df_before,df],axis = 0)

        df = df.drop_duplicates(keep="last",subset = "Open Time")
        df.to_csv(os.path.join("MarketData",pair_in,f"{pair_in}_{interval_in}.csv"),index = False)

    def getWhitelistPairsPastData(self):
        intervals = ["1m", "5m", "15m", "1h", "4h"]
        whitelist = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "APTUSDT"]
        for interval in intervals:
            for pair in whitelist:
                self.getPairAllPastData(pair, interval)

        self.FileIO.cutAndPasteFilesIntoDirectories()

    def datetime_to_unix(self, dates):
        return (dates - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')
