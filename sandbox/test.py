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
print(
    lv1 \
        .groupby(F.datepart(F.lit("YEAR"),"Date").alias("year")) \
        .agg(F.count_distinct("Ticker").alias('unique_tickers')) \
        .orderBy("Year").show(100)
)

from pyspark.sql import Window
from pyspark.sql.functions import row_number
window = Window.partitionBy("Ticker").orderBy("Date") 

lv2 = lv1.withColumn("RSI",calculate_rsi(F.col("Close")).over(window)).collect()

lv3 = lv2.where(lv2.Ticker == 'CBA.AX').collect()

lv3.orderBy("Date").show()