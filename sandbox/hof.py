import pyspark.sql.functions as F
from pyspark.sql import Window
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

    #df = spark.createDataFrame([(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'), (5, 'b')], ("n", 'c'))
    #w = Window.partitionBy('c')
    #print(df \
    #      .withColumn('listing',F.collect_list("n").over(w)) \
    #      .withColumn('agg_win',F.aggregate("listing",  F.lit(0.0), lambda acc, x: acc + x)).explain())


    # Sample data
    data = [
        (1, 1, 1.1),
        (1, 2, 1.0),
        (1, 3, 1.1),
        (1, 4, 2.0),
        (1, 5, 1.1),
        (1, 6, 1.1),
        (1, 7, 3.0),
        (1, 8, 1.1),
        (1, 9, 1.1),
        (1, 10, 4.0),
        (1, 11, 1.1),
        (1, 12, 5.0),
        (1, 13, 1.1),
        (1, 14, 1.1),
        (1, 15, 1.1),
        
        (2, 1, 1.1),
        (2, 2, 1.1)
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, ["Ticker", "Date", "value"])

    # Define window partitioned by "id" and ordered by "timestamp"
    window = Window.partitionBy("Ticker").orderBy("Date")
    
    period = 2
    decay = (period-1)/period
    weigh = 1/period

    column = 'value'
    import math
    precision = 0.001
    lookback =  math.ceil(math.log(precision)/math.log(decay))
    print(lookback)
    # Collect values into an ordered array
    collected = F.transform(
            F.array_agg(F.col(column)).over(window.rowsBetween(-lookback,0)),
            lambda x,i: 
                F.when( 
                    i==0, 
                    x*F.lit(decay)**F.lit(lookback)/F.lit(weigh) 
                ).otherwise(x*F.lit(decay)**F.lit(lookback-i))
    ).alias("_tmp")
    
    raw_val = F.aggregate(
        collected,
        F.lit(0.0),
        lambda acc,x: acc+x,
        lambda x: x*weigh
    )

    filtered_val = F.when(F.size(collected)==(lookback+1),raw_val).otherwise(F.lit(None))

    df_with_array = df.withColumn(
        "filtered_val",
        filtered_val
    )
    

    df_with_array.show(truncate=False)