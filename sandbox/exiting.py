from pyspark.sql import Window
import pyspark.sql.functions as F

import math
from operator import add
from functools import reduce

def exit_timestamp_per_ticker(entry_price_col, sig_col:str='Close', timestamp_col:str='Date', max_hold_period=10, max_gain=1.05, max_loss=0.95, window= None):
    "idea is to retain for max_hold_period, if this exceeds, exit on the next interval"
    future_price = F.array_agg(F.col(sig_col)).over(window.rowsBetween(0,max_hold_period+1))
    #future_timestamp = F.array_agg(F.col(timestamp_col)).over(window.rowsBetween(0,max_hold_period+1))
    
    first_signal = F.array_position(
        F.transform(
            future_price,
            lambda x,i: 
                F.when( (x/F.col(entry_price_col)>max_gain) | (x/F.col(entry_price_col)<max_loss) | (i==max_hold_period), 1 )
        ),
        1
    )
    return first_signal+1
    """below need to be refactored into slipperage calc somehow"""
    #next day close? slipping an entire day seems too much?
    
    #return F.get(future_timestamp,first_signal+1)
    exit_price = F.get(future_price,first_signal+1)
    return exit_price

def slipperage(slip_price_col, signal_location=0, max_hold_period=10, window= None):
    """
    #TODO this solution is not elegant and is too tightly coupled with exit signal
    # figure out how to fix this 

    slip price col be something like avg of open and close
    modify exit price to slip price
    e.g.
        slip_price_col = (open+high+low+close)/4
    """
    future_price = F.array_agg(F.col(slip_price_col)).over(window.rowsBetween(0,max_hold_period+1))
    return F.get(future_price,signal_location)

def commission_ratio():
    """
    add commission base on slipped exit price
    ig au commission is 0.05% for trade above $10,000, $5 below
    consider minimum trade capital of 5000, and potential for double charge, 
    let commission be maxed at 0.2%
    """
    return 0.002

def __seq_find_time_allowed_trades():
    """
    get all intervals for which a position is possibly taken
    """
    pass

def __window_find_best_trade():
    """
    within a given time frame, find the best stock (some metric like market value, trajectory of price, and)
    """