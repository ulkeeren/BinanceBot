from MarketDataStream import MarketDataFetcher
client = MarketDataFetcher()
client.getWhitelistPairsPastData()

client.updateWhitelistData("BTCUSDT","1m")
