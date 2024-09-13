import pandas as pd
import vectorbt as vbt
from pandas_ta.momentum import rsi
from pandas_ta.volatility import bbands
from pandas_ta.volatility import donchian
from pandas_ta.overlap import supertrend

################################################################
# stratejilerin dataframe column ismi window lenghte göre değişiyor, onu halletmek lazım



def construct_signals(data_in, rdf):
    signals = pd.DataFrame()
    signals["Open Time"] = data_in["Open Time"]
    signals["Long Signal"] = signals["Open Time"].isin(rdf["long_entries"])
    signals["Short Signal"] = signals["Open Time"].isin(rdf["short_entries"])
    signals["Exit Signal"] = signals["Open Time"].isin(rdf["exit"])
    signals = signals.set_index("Open Time")
    signals.index.name = ""
    return signals


def donchian_strat(data_in, window=14):
    # DCL_14_14  DCM_14_14  DCU_14_14
    donchian_df = donchian(data_in["High"], data_in["Low"], upper_length=window, lower_length=window)
    rdf = pd.DataFrame()
    rdf["long_entries"] = data_in[(data_in["Close"] <= donchian_df["DCU_" + str(window) + "_" + str(window)]) & (
                donchian_df["DCU_" + str(window) + "_" + str(window)] <= data_in["High"])]["Open Time"]  # Long entry
    rdf["exit"] = data_in[(data_in["Close"] <= donchian_df["DCM_" + str(window) + "_" + str(window)]) & (
                donchian_df["DCM_" + str(window) + "_" + str(window)] <= data_in["Open"]) | (
                                      data_in["Open"] <= donchian_df["DCM_" + str(window) + "_" + str(window)]) & (
                                      donchian_df["DCM_" + str(window) + "_" + str(window)] <= data_in["Close"])][
        "Open Time"]
    rdf["short_entries"] = data_in[(data_in["Low"] <= donchian_df["DCL_" + str(window) + "_" + str(window)]) & (
                donchian_df["DCM_" + str(window) + "_" + str(window)] <= data_in["Close"])]["Open Time"]  # Short entry
    signals = construct_signals(data_in, rdf)
    return signals


def bollinger_strat(data_in, window=14):
    # BBL_14_2.0	BBM_14_2.0	BBU_14_2.0
    bbands_signal = bbands(data_in["Close"], length=window)
    rdf = pd.DataFrame()
    rdf["Short Signal"] = data_in["Close"] >= bbands_signal["BBU_" + str(window) + "_2.0"]
    rdf["Long Signal"] = data_in["Close"] >= bbands_signal["BBL_" + str(window) + "_2.0"]
    rdf["Exit Signal"] = ((data_in["Close"] < bbands_signal["BBM_" + str(window) + "_2.0"]) & (
                bbands_signal["BBM_" + str(window) + "_2.0"] < data_in["Open"])) | (
                                     (bbands_signal["BBM_" + str(window) + "_2.0"] < data_in["Close"]) & (
                                         data_in["Open"] < bbands_signal["BBM_" + str(window) + "_2.0"]))
    rdf["Open Time"] = data_in["Open Time"]
    rdf = rdf.set_index("Open Time")
    rdf.index.name = ""
    return rdf


def rsi_strat(data_in, window=14, threshold=5):
    rsi_sig = rsi(data_in["Close"], lenght=window)
    rdf = pd.DataFrame()
    rdf["Short Signal"] = data_in["Close"] >= 80 + threshold
    rdf["Long Signal"] = data_in["Close"] <= 20 - threshold
    rdf["Exit Signal"] = (49 < rsi_sig) & (rsi_sig < 51)

    return rdf


def supertrend_strat(data_in, window=14):
    # SUPERT_14_3.0	SUPERTd_14_3.0	SUPERTl_14_3.0	SUPERTs_14_3.0
    super_trend = supertrend(data_in["High"], data_in["Low"], data_in["Close"], window)
    super_trend["Long Diffs"] = super_trend["SUPERTl_" + str(window) + "_3.0"].diff()
    super_trend["Short Diffs"] = super_trend["SUPERTs_" + str(window) + "_3.0"].diff()
    rdf = pd.DataFrame()
    rdf["Long Signal"] = super_trend["SUPERTl_" + str(window) + "_3.0"] > 0
    rdf["Short Signal"] = super_trend["SUPERTs_" + str(window) + "_3.0"] > 0
    # rdf["Exit Signal"] = (super_trend["SUPERTl_14_3.0"] == 0) | (super_trend["SUPERTs_14_3.0"]==0)
    rdf["Exit Signal"] = super_trend["SUPERTd_" + str(window) + "_3.0"] != super_trend[
        "SUPERTd_" + str(window) + "_3.0"].shift(1)
    return rdf


def backtest(data_in, sigs):
    if "Short Exit Signal" and "Long Exit Signal" in sigs.columns:
        pass
    else:
        return vbt.Portfolio.from_signals(data_in, entries=sigs["Long Signal"], short_entries=sigs["Short Signal"],
                                          exits=sigs["Exit Signal"], init_cash=100)


def optimize_strat(data_in, range_in, strat):
    # SuperTrend
    # Rsi
    # Bbands
    # Donchian
    mp = 0
    mpf = 0
    window = 0
    for i in range(range_in[0], range_in[1] + 1):
        s = strat(data_in, window=i)
        bt = backtest(data_in["Close"], s)
        try:
            if bt.calmar_ratio() > mp:
                mpf = bt
                mp = bt.calmar_ratio()
                window = i
        except:
            if bt.total_profit() > mp:
                mpf = bt
                mp = bt.total_profit()
                window = i
    if mpf == 0:
        return "Bad Strategy", "Bad Strategy"

    return mpf, window