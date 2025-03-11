import pyspark
#from ta.momentum import RSIIndicator
from talib import RSI
from pyspark.sql.functions import pandas_udf, PandasUDFType, to_date, from_unixtime
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
import pandas as pd

if __name__ == "__main__":
    # Convert to a Spark DataFrame
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder \
    .master("local") \
    .appName("udf") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .config("spark.python.profile.memory", "true") \
    .getOrCreate()
    #spark://<your master endpoint>


@pandas_udf('float', PandasUDFType.SCALAR)
def calculate_rsi(series: pd.Series) -> pd.Series:
    #rsi = RSIIndicator(series)
    #return rsi.rsi()
    rsi = RSI(series,14)
    ## https://stackoverflow.com/questions/68754755/how-does-talib-calculate-rsi-relative-strength-index
    ## says this uses SMMA under the hood
    return rsi
#@pandas_udtf('float', PandasUDFType.SCALAR)
#def calculate_macd(series: pd.Series) -> pd.DataFrame:
#    pass  


#df = spark.createDataFrame([[float(i%20)] for i in range(200)],"Close: float")#.show(100)
df = spark.read.load("histdata",format="parquet",pathGlobFilter="*.parquet")
#df = spark.read.load("histdata/2000-01-01_0.parquet",format="parquet")
#df.withColumn("RSI", df.Close + 1).show(100)
lv1 = df \
    .withColumn("Date_adj", F.to_date(F.from_unixtime(df.Date/1_000_000_000))) \
    .drop("Date") \
    .withColumnRenamed("Date_adj","Date") \
    .orderBy(["Date", "Ticker"], ascending=[True, True])
    #.sort(df.Date.asc())
#below line does not work (fixed via java downgrade from 21 to 17)
#df.withColumn("RSI", calculate_rsi(F.col("Close"))).show(200)

#print(lv1.groupby(F.datepart(F.lit("YEAR"),"Date").alias("year")).agg({'Ticker':'count'}).orderBy("Year").show(100))
#print(
#    lv1 \
#        .groupby(F.datepart(F.lit("YEAR"),"Date").alias("year")) \
#        .agg(F.count_distinct("Ticker").alias('unique_tickers')) \
#        .orderBy("Year").show(100)
#)

from pyspark.sql import Window
from pyspark.sql.functions import row_number, lag, greatest, expr, least
#window = Window.partitionBy("Ticker").orderBy("Date") 

#lv2 = lv1.withColumn("RSI",calculate_rsi(F.col("Close")).over(window)).collect()

#lv3 = lv2.where(lv2.Ticker == 'CBA.AX').collect()

#lv3.orderBy("Date").show()

# approximating ema with wma with defined precision
import time
def update_time(t):
    new_t = time.time()
    print(new_t-t)
    return new_t 

t = time.time()


import math
from operator import add
from functools import reduce
    

""" TODO need to adjust this such that the formula can correctly shrink when not enough for data for lookback
 why? 
 1. the weight always sum to 1
 2. more data
 3. enable usage for larger period in ema
    3.1 currently it would result in null value for those period instead. 

"""
def ema(column:str,period:int=14,type='standard',window=Window.partitionBy("Ticker").orderBy("Date")):
    """
    rollout calc
    weight check
        r, (1-r)
        r, (1-r)*r, (1-r)^2
        ...
        r(1,(1-r), ... ,(1-r)^n/r)
    sum of weight would always be 1
    the only thing lost would be information lost before than many step, 
    hence what's outside of lookback would contribute of only 0.001 precision within the EMA  

    decay^x<0.01
    x*lg(decay)<lg(0.01)
    x<lg(0.01)/lg(decay)
    """
    #period = 14
    if type == 'standard':
        decay = (period-1)/(period+1)
        weigh = 2/(period+1)
    elif type == 'wiler':
        decay = (period-1)/period
        weigh = 1/period
    lookback =  math.ceil(math.log(0.001)/math.log(decay))

    #for p in range(lookback):
    #    lv2 = lv2.withColumn(column+'_lw'+str(p),lag(column,p+1).over(window)* ((decay)**(p+1)) )
    lagged_terms = [
        (lag(F.col(column), p + 1).over(window) * (decay ** (p + 1))).alias(f"{column}_lw{p}")
        for p in range(lookback)
    ]
    #c1 = "+".join([column+'_lw'+str(p) for p in range(lookback)])
    #expr = "".join([str(weigh),"*(",column, "+", c1, "/", str(weigh), ")"])
    #return df.withColumns('ema_'+column,expr)
    # https://stackoverflow.com/questions/44502095/summing-multiple-columns-in-spark
    expr = reduce(add, [col if i<len(lagged_terms) else col/weigh for i,col in enumerate(lagged_terms,1)])
    return weigh*expr

#expression = "(_ad+" + c1 + ")/(Volume+" + c2 + ")"
#print(expression)

# calculating TMF with WMA
w = Window.partitionBy("Ticker").orderBy("Date")
#lv2 = lv1 \
#    .withColumn("_close_yesterday", lag("Close").over(w)) \
#    .withColumn("_trh",greatest("High","_close_yesterday")) \
#    .withColumn("_trl",least("Low","_close_yesterday")) \
#    .withColumn("_ad",(2*F.col("Close")-F.col("_trh")-F.col("_trl"))/(F.col("_trh")-F.col("_trl")+0.00000000001)*F.col("Volume") ) \
#    .withColumn("ema_ad", ema("_ad",period=14,type="wiler")) \
#    .withColumn("ema_volume", ema("Volume",period=14,type="wiler"))
#
#lv2 = lv2.withColumn("TMF", F.col("ema_ad")/F.col("ema_volume"))#.drop(*(["_ad_lw"+str(p) for p in range(lookback)]+["_Volume_lw"+str(p) for p in range(lookback)]))


# calculating MACD
lv2 = lv2 \
    .withColumn("ema_close_12", ema("Close",period=12)) \
    .withColumn("ema_close_26", ema("Close",period=26)) \
    .withColumn("macd", F.col("ema_close_12")-F.col("ema_close_26")) \
    .withColumn("macd_signal", ema("macd",period=9))




#lv2.where(lv2.Ticker == 'CBA.AX').show(300)
lv2.where(lv2.Ticker == 'CBA.AX').write.csv('cba_debug.csv',header=True, mode="overwrite")
#lv1.withColumn("_close_yesterday")
t = update_time(t)

spark.sparkContext.show_profiles()
#spark.profile()