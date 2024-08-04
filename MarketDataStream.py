import common_import

class MarketDataFetcher():
    def __init__(self,api_key=0,api_secret=0 ):
        self.base_endpoint = "https://fapi.binance.com"
        self.api_key = api_key
        self.api_secret = api_secret
        if api_key != 0 and api_secret != 0:
            self.client = UMFutures("iPQMe46exUy10KiBaBahQT7ow1uzb9jaxlKj19Bg5BI8JEwJL5bw9LCvJtfKuVbP",
                                    "J4FGJQdGPpNGm8jpnj3IuFuDJuAe7FIa3cM51L4z662UYux3qD0ByLJ0bfIltZvK")
    
    def get_pair_data(self,pair_in,interval_in,**kwargs):
        return self.convert_to_df(self.client.continuous_klines(pair_in,contractType="PERPETUAL", interval=interval_in,limit=1000,**kwargs))

    def convert_to_df(self, data):
        data_df=pd.DataFrame(np.asarray(data,dtype=np.float64),columns=["Open Time","Open","High","Low","Close","Volume","Close Time","Quote Asset Volume","Number of Trades","Taker Buy Volume","Taker Buy Quote Asset Volume","Ignore"])
        data_df['Open Time'] = pd.to_datetime(data_df['Open Time'], unit='ms')
        data_df['Close Time'] = pd.to_datetime(data_df["Close Time"],unit='ms').dt.ceil("min")
        data_df["Index"] = np.arange(len(data))
        return data_df.drop("Ignore",axis=1)