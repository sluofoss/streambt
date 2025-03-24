from pyspark.sql import Window
import pyspark.sql.functions as F

import math
from operator import add
from functools import reduce

def exit_sig_per_ticker(entry_price_col, sig_col:str='Close',max_hold_period=10, max_gain=1.05, max_loss=0.95, window= None):
    "idea is to retain for max_hold_period, if this exceeds, exit on the next interval"
    future_price = F.array_agg(F.col(sig_col)).over(window.rowsBetween(0,max_hold_period+1))
    first_signal = F.array_position(
        F.transform(
            future_price,
            lambda x,i: 
                F.when( (x/F.col(entry_price_col)>max_gain) | (x/F.col(entry_price_col)<max_loss) | (i==max_hold_period), 1 )
        ),
        1
    )
    """below need to be refactored into slipperage calc somehow"""
    #next day close? slipping an entire day seems too much?
    exit_price = F.get(future_price,first_signal+1)
    return exit_price