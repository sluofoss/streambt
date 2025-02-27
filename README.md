# streambt

## goal of project
- backtesting framework that supports streaming. i.e. capable of direct convert to production with minimal/no change. 
    - spark
    - flink
    - polars (when is streaming avail unknown)
    - ibis (sql compatibility frontend) has spark and flink backend
        - seems to have custom support of udf (degree of transcompile for different backend unknown and seems to be a difficult problem