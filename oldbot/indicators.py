import pandas as pd
import numpy as np
from pandas_ta.volatility import atr
class Indicators:
    def __init__(self):
        pass
    def average_true_range(self,data_in,period_in):
        return atr(data_in["High"],data_in["Low"],data_in["Close"])
    