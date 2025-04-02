# streambt

# dev setup
./install-talib.sh


## goal of project
- backtesting framework that supports streaming. i.e. capable of direct convert to production with minimal/no change. 
    - spark
    - flink
    - polars (when is streaming avail unknown)
    - ibis (sql compatibility frontend) has spark and flink backend
        - seems to have custom support of udf (degree of transcompile for different backend unknown and seems to be a difficult problem)


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


# Dataframe layered calculation
``` 
datafeed
indicators
strategy_calc (vectorized as much as possible AMAP)
    signal
broker cost
order/position

```
the dataframe will need a method to dynamically create indicator column names
the dataframe will need a method to dynamically track dependencies between indicators to prevent duplicate effort
    the unique identifier of a indicator need to consist of the following
        - source columns names(sorted)
        - indicator function signature
        - indicator additional parameters
this uuid need to apply to strategy as well
    strategy will compose of the following:
        function that parse multiple indicators to produce a pulse signal with strength (specialized indicator).
        function that parse the strength and current state of portfolio and generate a buy amount (specialized indicator)
- portfolio needs to be a column that evolves with strategy

code call should look like below

```
cerebro
data_uuid = cerebro.add_data(data)
indicator1_uuids = cerebro.add_indicator(func, data_uuid[col used], param)
indicator2_uuids = cerebro.add_indicator(func2, indicator1_uuids[col used], data_uuid+col, param)
pulse_indicator = cerebro.add_indicator(func3, indicator2_uuid[col used], data_uuid+col, param)

strat1_uuid = cerebro.add_strat(pulse, allocation_method)
 
```

strat allocation > broker interaction (commissions etc) (should be a function rather than col) > portfolio > strat allocation (needs to be done in a for loop, cant be easily vectorized?)

dataframe would look like below:
```
ts,t,ohlcv,id1...n,buy_sig,buy_price,sell_ts,slip_sell_price,additional_cost,gain_loss_ratio

buy_price = o[i+1]
gain_loss_ratio = buy_sig * [slip_sell_price/buy_price-commission_ratio]

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

## next step:
- convert yearly portfolio performance analysis to pyspark
- parametrize things like 
  - years of strategy consistency
  - max profit margin per trade
  - max loss per trade
- increase grain on performance analysis into trade opportunity level. (try to catch extreme gain events?)
- strategy agnostic metrics like past performance metrics like yearly return on hold to stock
- [x] improve tmf signal with using percentile on strength of dd 
- currently tallying win loss ratio of single instrument, change to focus on ranking product of all trades?
- 

## experimental result
- naively introducing median filter or dd count filter made the signal worst
- tmf26w are more reliable wrt w l ratio than tmf4w