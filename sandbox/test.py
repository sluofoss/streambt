import pyspark
#from ta.momentum import RSIIndicator
#import pyspark.pandas
#import pyspark.pandas.window
from talib import RSI
from pyspark.sql.functions import pandas_udf, PandasUDFType, to_date, from_unixtime
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
import pandas as pd
from ema import ema_w, ema_w_a, ema_w_a_hof
from exiting import exit_timestamp_per_ticker, slipperage, commission_ratio

if __name__ == "__main__":
    # Convert to a Spark DataFrame
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
    .master("local[6]") \
    .appName("udf") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .config("spark.python.profile.memory", "true") \
    .config("spark.driver.memory", "8g") \
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
window = Window.partitionBy("Ticker").orderBy("Date") 

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



import re

""" TODO need to adjust this such that the formula can correctly shrink when not enough for data for lookback
 why? 
 1. the weight always sum to 1
 2. more data
 3. enable usage for larger period in ema
    3.1 currently it would result in null value for those period instead. 

"""

#expression = "(_ad+" + c1 + ")/(Volume+" + c2 + ")"
#print(expression)

# --------------------
# calculating TMF with WMA
# --------------------

w = Window.partitionBy("Ticker").orderBy("Date")
lv2 = lv1
lv2 = lv2 \
    .withColumn("_close_yesterday", F.lag("Close").over(w)) \
    .withColumn("_trh",F.greatest("High","_close_yesterday")) \
    .withColumn("_trl",F.least("Low","_close_yesterday")) \
    .withColumn("_ad",(2*F.col("Close")-F.col("_trh")-F.col("_trl"))/(F.col("_trh")-F.col("_trl")+0.00000000001)*F.col("Volume") ) \
    .withColumn("ema_ad", ema_w_a_hof("_ad",period=14,type="wiler")) \
    .withColumn("ema_volume", ema_w_a_hof("Volume",period=14,type="wiler")) \
    .withColumn("TMF_w", F.col("ema_ad")/F.col("ema_volume")) \
    .withColumn("TMF_4w_min", F.min(F.col("TMF_w")).over(w.rowsBetween(-4*5,0)) ) \
    .withColumn("TMF_4w_min_dd", F.col("TMF_4w_min")-F.lag(F.col("TMF_4w_min"),1).over(w) ) \
    .withColumn("TMF_26w_min", F.min(F.col("TMF_w")).over(w.rowsBetween(-26*5,0)) ) \
    .withColumn("TMF_26w_min_dd", F.col("TMF_26w_min")-F.lag(F.col("TMF_26w_min"),1).over(w) ) \
    .withColumn("TMF_Simple_Signal", F.when( (F.col("TMF_26w_min_dd")==0) & (F.lag(F.col("TMF_26w_min_dd"),1).over(w)<0),1).otherwise(0) ) \
    .withColumn("debug_1", F.lag(F.col("TMF_26w_min_dd"),1).over(w)) \
    \
    \
    .withColumn("entry_price", F.lead((F.col("Open")+F.col("High")+F.col("Low")+F.col("Close"))/4,1).over(w)  ) \
    .withColumn("exit_index_from_now", exit_timestamp_per_ticker('entry_price',"Close","Date",10,1.05,0.95,w)) \
    .withColumn("slip_price_col", (F.col("High")+F.col("Low"))/2 ) \
    .withColumn("exit_slipping_price", slipperage("slip_price_col","exit_index_from_now",10+2,w ) ) \
    .withColumn("gain_loss_ratio", F.col('exit_slipping_price')/F.col("entry_price")-F.lit(commission_ratio()))

    
    #exit_timestamp_per_ticker, slipperage, commission_ratio

    #.withColumn("ema_ad", ema_w("_ad",period=14,type="wiler")) \
    #.withColumn("ema_volume", ema_w("Volume",period=14,type="wiler")) \
    #.withColumn("TMF_w", F.col("ema_ad")/F.col("ema_volume")) \
    #.withColumn("ema_ad_wa", ema_w_a("_ad",period=14,type="wiler")) \
    #.withColumn("ema_volume_wa", ema_w_a("Volume",period=14,type="wiler")) \
    #.withColumn("TMF_wa", F.col("ema_ad_wa")/F.col("ema_volume_wa")) \
    #.withColumn("TMF_4w_min", F.min(F.col("TMF_wa")).over(w.rowsBetween(-4*5,0)) ) \
    #.withColumn("TMF_8w_min", F.min(F.col("TMF_wa")).over(w.rowsBetween(-8*5,0)) ) \
    #.withColumn("TMF_26w_min", F.min(F.col("TMF_wa")).over(w.rowsBetween(-26*5,0)) ) \
    #.withColumn("TMF_26w_min_dd", F.col("TMF_26w_min")-F.lag(F.col("TMF_26w_min"),1).over(w) ) \




#.drop(*(["_ad_lw"+str(p) for p in range(lookback)]+["_Volume_lw"+str(p) for p in range(lookback)]))

# --------------------
# calculating MACD
# --------------------

#lv2 = lv1.withColumns(ema("Close",period=5,debug=True)).withColumns(ema("Close",period=10,debug=True))

#lv2 = lv2 \
#    .withColumn("ema_close_12", ema_w("Close",period=12)) \
#    .withColumn("ema_close_26", ema_w("Close",period=26)) \
#    .withColumn("macd", F.col("ema_close_12")-F.col("ema_close_26")) \
#    .withColumn("macd_signal", ema_w("macd",period=9))


#lv2 = lv2 \
#    .withColumn("ema_close_12_wa", ema_w_a("Close",period=12)) \
#    .withColumn("ema_close_26_wa", ema_w_a("Close",period=26)) \
#    .withColumn("macd_wa", F.col("ema_close_12_wa")-F.col("ema_close_26_wa")) \
#    .withColumn("macd_signal_wa", ema_w_a("macd_wa",period=9))

lv2 = lv2 \
    .withColumn("ema_close_12_hof", ema_w_a_hof("Close",period=12)) \
    .withColumn("ema_close_26_hof", ema_w_a_hof("Close",period=26)) \
    .withColumn("macd_hof", F.col("ema_close_12_hof")-F.col("ema_close_26_hof")) \
    .withColumn("macd_signal_hof", ema_w_a_hof("macd_hof",period=9))

#lv2 = lv2.withColumn('true_ema',.mean())

# --------------------
# calculating exit result for all points
# --------------------

#lv2 = lv2 \
#    .withColumn("over_gain_thres",F.lag)


# TODO, too large a period cause it to break, *10 *20 does not work.
# time to compare the performance difference between using lookback at different precision rate (0.001 vs 0.1 vs plain period)
#lv2 = lv2 \
#    .withColumn("ema_close_12_20", ema("Close",period=12*20)) \
#    .withColumn("ema_close_26_20", ema("Close",period=26*20)) \
#    .withColumn("macd_20", F.col("ema_close_12_20")-F.col("ema_close_26_20")) \
#    .withColumn("macd_signal_20", ema("macd_20",period=9*20))

#recs = lv2.collect()

# export full thing
#lv2.write.csv('full_df_debug.csv',header=True, mode="overwrite")


#lv2.where(lv2.Ticker == 'CBA.AX').show(300)
lv2.where(lv2.Ticker == 'CBA.AX').write.csv('cba_debug_2.csv',header=True, mode="overwrite")
#lv1.withColumn("_close_yesterday")
t = update_time(t)

spark.sparkContext.show_profiles()
#spark.profile()