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
        (1, 1, "A"),
        (1, 2, "B"),
        (1, -1, "C"),
        (2, 1, "X"),
        (2, 2, "Y")
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, ["id", "timestamp", "value"])

    # Define window partitioned by "id" and ordered by "timestamp"
    window = Window.partitionBy("id").orderBy("timestamp")

    # Collect values into an ordered array
    df_with_array = df.withColumn(
        "ordered_values",
        F.collect_list("value").over(window)
    )

    df_with_array.show(truncate=False)