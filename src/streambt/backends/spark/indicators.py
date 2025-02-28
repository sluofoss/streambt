import pyspark
#from ta.momentum import RSIIndicator
from talib import RSI
from pyspark.sql.functions import pandas_udf, PandasUDFType
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


df = spark.createDataFrame([[float(i%20)] for i in range(200)],"Close: float")#.show(100)
#df.withColumn("RSI", df.Close + 1).show(100)

#below line does not work (fixed via java downgrade from 21 to 17)
df.withColumn("RSI", calculate_rsi(F.col("Close"))).show(200)
