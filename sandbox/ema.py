from pyspark.sql import Window
import pyspark.sql.functions as F

import math
from operator import add
from functools import reduce

def ema_w(column:str,period:int=14,type='standard',window=None,debug=False,use_period_as_lookback=False):
    """
    rollout calc
    weight check
        r is smoothing rate
        r, (1-r)
        r, (1-r)*r, (1-r)^2
        ...
        r(1,(1-r), ... ,(1-r)^n/r)
    sum of weight would always be 1
    the only thing lost would be information lost before than many step, 
    hence what's outside of lookback would contribute of only 0.001 precision within the EMA  

    decay^x/weigh<0.01
    x*lg(decay)<lg(0.01*weigh)
    x<lg(0.01*weigh)/lg(decay)
    
    correctly accounting weigh actual making lookback larger. this would be very problematic. there needs to be a way to effectively do recursion/ema!
    """
    if window is None:
        window = Window.partitionBy("Ticker").orderBy("Date")
    precision = 0.999999
    #period = 14
    if type == 'standard':
        decay = (period-1)/(period+1)
        weigh = 2/(period+1)
    elif type == 'wiler':
        decay = (period-1)/period
        weigh = 1/period
    
    lookback =  math.ceil(math.log(precision*weigh)/math.log(decay)) if use_period_as_lookback is False else period
    if debug:
        print("compare lookback")
        print(period,lookback)
        all_w1 = [weigh]+[decay ** (p + 1) * weigh for p in range(period)]
        all_w1[-1] /= weigh
        print(all_w1) 
        all_w2 = [weigh]+[decay ** (p + 1) * weigh for p in range(math.ceil(math.log(precision*weigh)/math.log(decay)))]
        all_w2[-1] /= weigh
        print(all_w2)
        
        print(sum(all_w2[len(all_w1):]))

        print("finish compare")
    #for p in range(lookback):
    #    lv2 = lv2.withColumn(column+'_lw'+str(p),lag(column,p+1).over(window)* ((decay)**(p+1)) )
    lagged_terms = [F.col(column)]+[
        (F.lag(F.col(column), p + 1).over(window) * (decay ** (p + 1))).alias(f"{column}_{period}_lw{p}")
        for p in range(lookback)
    ]
    #c1 = "+".join([column+'_lw'+str(p) for p in range(lookback)])
    #expr = "".join([str(weigh),"*(",column, "+", c1, "/", str(weigh), ")"])
    #return df.withColumns('ema_'+column,expr)
    # https://stackoverflow.com/questions/44502095/summing-multiple-columns-in-spark
    expr = reduce(add, [col if i<len(lagged_terms) else col/weigh for i,col in enumerate(lagged_terms,1)])

    if debug:
        print(period, decay, weigh)
        all_w = [1]+[decay ** (p + 1) for p in range(lookback)]
        all_w[-1] /= weigh
        print(sum(all_w)*weigh, all_w) 
        return {f'ema_{column}_{period}':weigh*expr, **{str(t)[-10:]:t for t in lagged_terms}}
    else:
        return weigh*expr

def ema_w_a(column:str,period:int=14,type='standard',window= None, debug=False,use_period_as_lookback=False):
    """
    rollout calc
    weight check
        r is smoothing rate
        r, (1-r)
        r, (1-r)*r, (1-r)^2
        ...
        r(1,(1-r), ... ,(1-r)^n/r)
    sum of weight would always be 1
    the only thing lost would be information lost before than many step, 
    hence what's outside of lookback would contribute of only 0.001 precision within the EMA  

    decay^x/weigh<0.01
    x*lg(decay)<lg(0.01*weigh)
    x<lg(0.01*weigh)/lg(decay)
    
    correctly accounting weigh actual making lookback larger. this would be very problematic. there needs to be a way to effectively do recursion/ema!
    """

    if window is None:
        window = Window.partitionBy("Ticker").orderBy("Date")
    precision = 0.999999
    #period = 14
    if type == 'standard':
        decay = (period-1)/(period+1)
        weigh = 2/(period+1)
    elif type == 'wiler':
        decay = (period-1)/period
        weigh = 1/period
    
    lookback =  math.ceil(math.log(precision*weigh)/math.log(decay)) if use_period_as_lookback is False else period
    if debug:
        print("compare lookback")
        print(period,lookback)
        all_w1 = [weigh]+[decay ** (p + 1) * weigh for p in range(period)]
        all_w1[-1] /= weigh
        print(all_w1) 
        all_w2 = [weigh]+[decay ** (p + 1) * weigh for p in range(math.ceil(math.log(precision*weigh)/math.log(decay)))]
        all_w2[-1] /= weigh
        print(all_w2)
        
        print(sum(all_w2[len(all_w1):]))

        print("finish compare")
    #for p in range(lookback):
    #    lv2 = lv2.withColumn(column+'_lw'+str(p),lag(column,p+1).over(window)* ((decay)**(p+1)) )
    lagged_terms = [F.col(column)]+[
        (F.lag(F.col(column), p + 1).over(window) * (decay ** (p + 1))).alias(f"{column}_{period}_lw{p}")
        for p in range(lookback-1)
    ] + [F.lag( F.avg(F.col(column)).over(window.rowsBetween(-period,0)) ).over(window) * (decay ** (lookback))]
    #c1 = "+".join([column+'_lw'+str(p) for p in range(lookback)])
    #expr = "".join([str(weigh),"*(",column, "+", c1, "/", str(weigh), ")"])
    #return df.withColumns('ema_'+column,expr)
    # https://stackoverflow.com/questions/44502095/summing-multiple-columns-in-spark
    expr = reduce(add, [col if i<len(lagged_terms) else col/weigh for i,col in enumerate(lagged_terms,1)])

    if debug:
        print(period, decay, weigh)
        all_w = [1]+[decay ** (p + 1) for p in range(lookback)]
        all_w[-1] /= weigh
        print(sum(all_w)*weigh, all_w) 
        return {f'ema_{column}_{period}':weigh*expr, **{str(t)[-10:]:t for t in lagged_terms}}
    else:
        return weigh*expr