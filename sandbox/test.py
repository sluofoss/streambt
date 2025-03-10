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
w = Window.partitionBy("Ticker").orderBy("Date")
lv2 = lv1 \
    .withColumn("_close_yesterday", lag("Close").over(w)) \
    .withColumn("_trh",greatest("High","_close_yesterday")) \
    .withColumn("_trl",least("Low","_close_yesterday")) \
    .withColumn("_ad",(2*F.col("Close")-F.col("_trh")-F.col("_trl"))/(F.col("_trh")-F.col("_trl")+0.00000000001)*F.col("Volume") ) 


# approximating ema with wma with defined precision
import time
def update_time(t):
    new_t = time.time()
    print(new_t-t)
    return new_t 

t = time.time()

period = 14
decay = (period-1)/period
import math
lookback =  math.ceil(math.log(0.001)/math.log(decay))
#decay^x<0.01
#x*lg(decay)<lg(0.01)
#x<lg(0.01)/lg(decay)
#lookback = 4
for p in range(lookback):
    lv2 = lv2.withColumn("_ad_lw"+str(p),lag("_ad",p+1).over(w)* ((decay)**(p+1)) ) \
             .withColumn("_Volume_lw"+str(p),lag("Volume",p+1).over(w)* ((decay)**(p+1)) )
             #.withColumn("Close"+str(p),lag("_ad",p+1).over(w)* ((decay)**(p+1)) )

c1 = "+".join(["_ad_lw"+str(p) for p in range(lookback)])
c2 = "+".join(["_Volume_lw"+str(p) for p in range(lookback)]) 
expression = "(_ad+" + c1 + ")/(Volume+" + c2 + ")"
#print(expression)
lv2 = lv2.withColumn("TMF", expr(expression))#.drop(*(["_ad_lw"+str(p) for p in range(lookback)]+["_Volume_lw"+str(p) for p in range(lookback)]))


#lv2.where(lv2.Ticker == 'CBA.AX').show(300)
lv2.where(lv2.Ticker == 'CBA.AX').write.csv('cba_debug.csv',header=True, mode="overwrite")
#lv1.withColumn("_close_yesterday")
t = update_time(t)

spark.sparkContext.show_profiles()
#spark.profile()