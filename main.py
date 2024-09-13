from MarketDataStream import MarketDataFetcher
import pandas as pd
"""client = MarketDataFetcher()
client.getWhitelistPairsPastData()

client.updateAllWhitelistData()"""
print(pd.read_csv("MarketData/BTCUSDT/BTCUSDT_1m.csv").iloc[-4])

