# streambt

## goal of project
- backtesting framework that supports streaming. i.e. capable of direct convert to production with minimal/no change. 
    - spark
    - flink
    - polars (when is streaming avail unknown)
    - ibis (sql compatibility frontend) has spark and flink backend
        - seems to have custom support of udf (degree of transcompile for different backend unknown and seems to be a difficult problem


# structure 
```
engine
    engine_abstract
broker
    broker_abstract
        txn_cost 
indicator
    indicator_abstract
strategy
    strategy_abstract
        Signal
order/position
metrics
    metrics_abstract
datafeed
    datafeed_abstract
backend
    spark
        engine
        indicator
            macd
            rsi
            etc
        strategy
    flink
        ...
```


Dataframe layered calculation
``` 
datafeed
indicators
strategy_calc (vectorized as much as possible AMAP)
    signal
broker cost
order/position
```



# stuff avail in backtrader for reference
- Cerebro (Y)
- Data Feeds (Y)
- Strategy (Y)
- Indicators (Y)
- Orders (Y)
- Broker (Y)
- Commission Schemes (part of broker for now)
- Analyzers (metrics?)
- Observers
- Sizers 
- Live Trading
- Plotting
- Datetime
- Automated Running