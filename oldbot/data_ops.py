from binance.um_futures import UMFutures
import requests
import pandas as pd
import os
import math
from pandas_ta.volatility import atr
from pandas_ta.volatility import donchian
from pandas_ta.volatility import bbands
from pandas_ta.overlap import supertrend
import numpy as np
from pandas import Timestamp
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler
import time

class DataGetter:
    def __init__(self,api_key=0,api_secret=0 ):
        self.base_endpoint = "https://fapi.binance.com"
        self.api_key = api_key
        self.api_secret = api_secret
        if api_key != 0 and api_secret != 0:
            self.client = UMFutures("iPQMe46exUy10KiBaBahQT7ow1uzb9jaxlKj19Bg5BI8JEwJL5bw9LCvJtfKuVbP","J4FGJQdGPpNGm8jpnj3IuFuDJuAe7FIa3cM51L4z662UYux3qD0ByLJ0bfIltZvK")

    def fetch_whitelist_data(self):
        intervals = ["1m","5m","15m","1h","4h"]
        whitelist = open(r"D:\binanceapi\whitelist.txt")
        for l in whitelist:
            for interval in intervals:
                self.get_all_pair_data_past(pair_in=l[:-1],interval_in=interval)

    def replace_market_file(self,pair_in,interval_in):
        os.remove(f"D:\\binanceapi\\MarketData\\{pair_in}\\{pair_in}_{interval_in}.csv")
        os.rename(f"D:\\binanceapi\\{pair_in}_{interval_in}.csv", f"D:\\binanceapi\\MarketData\\{pair_in}\\{pair_in}_{interval_in}.csv")
    
    def delete_duplicates(self,pair_in,interval_in):
        try:
            df = pd.read_csv(f"D:\\binanceapi\\MarketData\\{pair_in}\\{pair_in}_{interval_in}.csv")
            df=df.drop_duplicates(keep="last",subset = "Open Time")
            df.to_csv(f"{pair_in}_{interval_in}.csv",index = False)
            self.replace_market_file(pair_in,interval_in)

        except FileNotFoundError:
            pass
        
    def get_all_pair_data_past(self,**kwargs):
        if not os.path.exists(f"D:\\binanceapi\\MarketData\\{kwargs["pair_in"]}\\{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv"):
            print(f"D:\\binanceapi\\MarketData\\{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv")
            df=pd.DataFrame()
            try:
                df = self.get_pair_data(**kwargs)
            except:
                print(f"Can't fetch {kwargs["pair_in"]}")
                return
            while True:
                try:
                    unix=self.datetime_to_unix(df["Open Time"])
                    df_before = self.get_pair_data(pair_in,interval_in=kwargs["interval_in"],endTime=unix[0])
                    df=pd.concat([df_before,df],axis = 0)
                except:
                    break
            df.to_csv(f"{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv",index = False)
            try:
                os.rename(f"D:\\binanceapi\\{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv", f"D:\\binanceapi\\MarketData\\{kwargs["pair_in"]}\\{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv")
            except FileNotFoundError:
                os.makedirs(f"D:\\binanceapi\\MarketData\\{kwargs["pair_in"]}")
                os.rename(f"D:\\binanceapi\\{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv", f"D:\\binanceapi\\MarketData\\{kwargs["pair_in"]}\\{kwargs["pair_in"]}_{kwargs["interval_in"]}.csv")
            return df

    def get_pair_data(self,pair_in,interval_in,**kwargs):
        return self.convert_to_df(self.client.continuous_klines(pair_in,contractType="PERPETUAL", interval=interval_in,limit=1000,**kwargs))

    def update_all_data(self):
        whitelist = open(r"D:\binanceapi\whitelist.txt")
        intervals = ["1m","5m","15m","1h","4h"]
        flag = True
        whitelist.seek(0)
        df = pd.DataFrame()
        for l in whitelist:
            for interval in intervals:
                try:
                    desired_data = pd.read_csv(f"D:\\binanceapi\\MarketData\\{l[:-1]}\\{l[:-1]}_{interval}.csv")
                except FileNotFoundError:
                    flag = False
                if flag:
                    if interval.endswith("m"):
                        current_time = pd.to_datetime(self.client.time()["serverTime"],unit="ms").floor(f"{interval}in")
                    else:
                        current_time = pd.to_datetime(self.client.time()["serverTime"],unit="ms").floor(f"{interval}")
                    time_delta= current_time - pd.Timestamp(desired_data.iloc[-1]["Close Time"])
                    limit = time_delta / pd.Timedelta(interval)
                    for i in range(math.ceil(limit/1000)):
                        df_late = self.get_pair_data(pair_in=l[:-1], interval_in=interval, endTime = current_time - i*1000*pd.Timedelta(interval))
                        df=pd.concat([df_late,df],axis = 0)
                    desired_data= pd.concat([desired_data,df],axis = 0)
                    desired_data.drop_duplicates(keep="last",subset = "Open Time",inplace = True)
                    desired_data.to_csv(f"{l[:-1]}_{interval}.csv",index = False)
                    self.replace_market_file(pair_in=l[:-1], interval_in=interval)
                    #TODO: Still not removing duplicates

            
    def convert_to_df(self, data):
        data_df=pd.DataFrame(np.asarray(data,dtype=np.float64),columns=["Open Time","Open","High","Low","Close","Volume","Close Time","Quote Asset Volume","Number of Trades","Taker Buy Volume","Taker Buy Quote Asset Volume","Ignore"])
        data_df['Open Time'] = pd.to_datetime(data_df['Open Time'], unit='ms')
        data_df['Close Time'] = pd.to_datetime(data_df["Close Time"],unit='ms').dt.ceil("min")
        data_df["Index"] = np.arange(len(data))
        return data_df.drop("Ignore",axis=1)
    
    def backtesting_preprocessing(self,data_in):
        returnDF = data_in[["Open","High","Low","Close","Volume","Open Time"]].set_index("Open Time")
        returnDF.index.name=""
        #returnDF["Open Time"] = returnDF.index.values
        return returnDF
        
    def datetime_to_unix(self,dates):
        return (dates - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')
        
    def get_valid_pairs(self):
        exchange_info_endpoint = self.base_endpoint + "/fapi/v1/exchangeInfo"
        response = requests.get(exchange_info_endpoint)
        if response.status_code == 200:
            exchange_info = response.json()
            return [symbol["symbol"] for symbol in exchange_info['symbols']]

    

    def low_swings(self,data_in,order=2):
        low_swing_list = []
        for i in range(order,len(data_in)-order):
            flag = True
            for j in range(1,order+1):
                if data_in.iloc[i]["Low"] > data_in.iloc[i+j]["Low"] or data_in.iloc[i]["Low"] > data_in.iloc[i-j]["Low"]:
                    flag=False
                    break
            if flag == True:
                low_swing_list.append(data_in.iloc[i]["Index"])
            
        return low_swing_list
    
    def high_swings(self,data_in,order=2):
        high_swing_list = []
        for i in range(order,len(data_in)-order):
            flag = True
            for j in range(1,order+1):
                if data_in.iloc[i]["High"] < data_in.iloc[i+j]["High"] or data_in.iloc[i]["High"] < data_in.iloc[i-j]["High"]:
                    flag=False
                    break
            if flag == True:
                high_swing_list.append(data_in.iloc[i]["Index"])
            
        return high_swing_list

    def line_opt(self,line,data_in,mode="Low"):
        #line: list containing slope [0] and intercept [1] of a  line
        dif_list = data_in[mode] - (line[0]*data_in[mode]+line[1])
        min_dif_idx = dif_list.argmin()
        min_dif = dif_list.iloc[min_dif_idx]
        return [(min_dif/min_dif_idx) - line[0] , line[1]] 
    
    def init_line(self, data_in,mode_in="Low"):
        #TODO: gives a decent line but needs optimization
        reg = LinearRegression()
        #X=np.arange(len(data_in)).reshape(-1, 1)
        X=data_in["Index"].values.reshape(-1, 1)
        y=data_in[mode_in].values.reshape(-1,1)
        reg.fit(X,y)
        return reg.coef_[0][0],reg.intercept_[0]
        
    #TODO: Herhangi bir candlestickin open-close arasından geçiyor bir line onun kontrolünü yapan fonksiyon
    def check_oc_collision_of_single_candlestick(self,arg,line,mode="Open Time"):
        """
        input
            arg: bir pandas dataframeinin rowu da olabilir (pandas.core.series.Series),    
                  index de olabilir (int),
                  tarih de olabilir (pandas._libs.tslibs.timestamps.Timestamp)
                bunu şimdilik bir trendline çizdiğimizde trendline başlangiç mumundan itibaren ilerdeki diğer mumlarla çakişip çakişmadiğini bir for loop içinde
                bulmak için kullanicaz,
                TODO: ileride bunu trendline başlangic mumunu tespit edebildiğimde trendline mumunu doğrulamak için kullanicaz

            line:
                regresyon sonucu elde ettiğimiz slope ve intercept, slope'u indexle çarpinca regresyon linei çikiyor.
                yani diğer data tiplerini timestampe çevirmek lazim
            mode:
                Open Time ya da Close Time'a göre algoritmayi çaliştirmak için aldiğimiz bir parametre
        """
        idx = 0
        #dataframe row case
        if type(arg) == "pandas.core.series.Series":
            idx = self.data[self.data["Open Time"] == arg["Open Time"]]["Index"]

        #int case
        elif type(arg) == "int":
            idx=arg

        #timestamp case
        elif type(arg)=="pandas._libs.tslibs.timestamps.Timestamp":
            idx = self.data[self.data["Open Time"] == arg].index
        
        return (    [self.data.iloc[idx]["Open Time"] ,self.data.iloc[idx]["Close"] <= line[0]*idx+line[1] <= self.data.iloc[idx]["Open"] 
                    or
                    self.data.iloc[idx]["Open"] <= line[0]*idx+line[1] <= self.data.iloc[idx]["Close"]]    
                )
    
    def check_oc_collisions(self,begin,end,line):
        collision_list = [] #bu liste [bool,timestamp] veri türünde olacak şekilde candlesticklerin istenen linela çakışıp çakışmadığını tutacak 
        begin_idx = begin
        while begin_idx != end+1:
            collision_list.append(self.check_oc_collision_of_single_candlestick(begin_idx,line))
            begin_idx += 1

        return collision_list
    
    def find_support_in_chunk(self,data_in,order=15):
        smallest_15 = data_in.nsmallest(order,"Low")
        return np.sum(smallest_15["Low"].values.reshape(-1,1)) / order 
        #average_order = np.sum(smallest_15["Low"].values.reshape(-1,1)) / order
        #return smallest_15.nsmallest(1,"Index").iloc[0], smallest_15.nlargest(1,"Index").iloc[0], average_order

    def find_resistance_in_chunk(self,data_in,order=15):
        largest_15 = data_in.nlargest(order,"High")
        return np.sum(largest_15["High"].values.reshape(-1,1)) / order
        #average_order = np.sum(largest_15["High"].values.reshape(-1,1)) / order
        #return largest_15.nsmallest(1,"Index").iloc[0], largest_15.nlargest(1,"Index").iloc[0], average_order
    
    def find_sr_info(self,data_in,order = 15,chunk_size = 100):
        support_info = {
            "order" : order,
            "chunk_size" : chunk_size,
            "chunk_begins_n_ends" : [] #chunk beginning candle time, chunk ending candle time, value    
        }
        resistance_info = {
            "order" : order,
            "chunk_size" : chunk_size,
            "chunk_begins_n_ends" : [] #chunk beginning candle time, chunk ending candle time, value    
        }

        for i in range(chunk_size,len(data_in)):
            support_info["chunk_begins_n_ends"].append([data_in.iloc[i-chunk_size],data_in.iloc[i],self.find_support_in_chunk(data_in.iloc[i-chunk_size : i])])
            resistance_info["chunk_begins_n_ends"].append([data_in.iloc[i-chunk_size],data_in.iloc[i],self.find_resistance_in_chunk(data_in.iloc[i-chunk_size : i])])
            
        return support_info, resistance_info

    def isIncreasing(self,data_in):
        midPoints = (data_in["Close"] + data_in["Open"])/2

    
    """
    def get_pair_data(self,pair,interval,limit=500):
        
        return requests.get("https://fapi.binance.com/fapi/v1/klines?symbol={pair}&interval={interval}&limit={limit}")  bu 1. secenek
        r = requests.get("https://fapi.binance.com/fapi/v1/klines?symbol=BAKEUSDT&interval=1m&limit=500")
        print(r.json())
        bu çalışıyor

    def data(self,pair,interval,limit=500):
        return self.convert_to_df(requests.get("https://fapi.binance.com/fapi/v1/klines?symbol={pair}&interval={interval}&limit={limit}").json())
    """
    
    def ema(self,data_in,mode_in="Low", order = 15):
        line_values = []
        dates = []
        c=1
        for idx in range(order,len(data_in)):
            t=0       
            for value in data_in.iloc[ idx-order : idx ][mode_in]:
               t += value
            line_values.append(t / order)
            dates.append(data_in.iloc[idx]["Open Time"])
        return dates,line_values
    
    def atr(self,data_in,window=14):
        return atr(data_in["High"],data_in["Low"],data_in["Close"],lenght = window )
    
    def Bollinger(self,data_in,window=14):
        return bbands(data_in["Close"],length=window)
    
    def donchian(self,data_in,window=14):
        return donchian(data_in["High"],data_in["Low"],upper_length=window, lower_length=window)
    
    def superTrend(self,data_in,window=14):
        return supertrend(data_in["High"],data_in["Low"],data_in["Close"],window)
        