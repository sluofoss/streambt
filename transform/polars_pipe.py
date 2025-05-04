import polars as pl
import polars.datatypes as DTypes
import inspect


from indicators import *

print('start script')


print('start read')

lazy = False
if lazy:
    df = pl.scan_parquet(source = f'histdata', schema={
        'Date'          :DTypes.Datetime('ns'),
        'Ticker'        :DTypes.String,
        'Adj Close'     :DTypes.Float64,
        'Close'         :DTypes.Float64,
        'Dividends'     :DTypes.Float64,
        'High'          :DTypes.Float64,
        'Low'           :DTypes.Float64,
        'Open'          :DTypes.Float64,
        'Stock Splits'  :DTypes.Float64,
        'Volume'        :DTypes.Float64,
    }, allow_missing_columns=True)
else:
    df = pl.read_parquet(source = f'histdata', columns={
        'Date'          :DTypes.Datetime('ns'),
        'Ticker'        :DTypes.String,
        'Adj Close'     :DTypes.Float64,
        'Close'         :DTypes.Float64,
        'Dividends'     :DTypes.Float64,
        'High'          :DTypes.Float64,
        'Low'           :DTypes.Float64,
        'Open'          :DTypes.Float64,
        'Stock Splits'  :DTypes.Float64,
        'Volume'        :DTypes.Float64,
    }, allow_missing_columns=True)
lv1 = df.sort('Date',"Ticker")

print('start indicators')

over_params = {'partition_by':'Ticker', 'order_by':"Date"}

lv2 = lv1.with_columns(
    pl.col("Date").rank(method = 'dense').over(**over_params).alias("Date"),
    pl.col("Date").alias("Date_str")
)


# -------------------------------- calculate general exit return

print('start entry exit calc')
lv2 = (
    lv2.with_columns(
        entry_price= ((pl.col("Open")+pl.col("High")+pl.col("Low")+pl.col("Close"))/4)#.shift(-1).over(**over_params),
    )
)
#.filter(pl.col('Ticker')=='CBA.AX').select('Date','Ticker','Close','ema_close_12_wilder')

#focus = lv2.to_pandas()

#import plotly.express as px
#px.line(focus, x = 'Date', y=['Close','ema_close_12_wilder'])


lv2 = exit_signal_statistics_ts_by_ticker(lv2,max_hold_period=10)

# ------------------------------ other indicators

lv2 = (
    lv2
    #---------------------------------macd
    .with_columns(
        ema_close_12_wilder = pl.col('Close').ewm_mean(com=12-1).over(**over_params),
        ema_close_26_wilder = pl.col('Close').ewm_mean(com=26-1).over(**over_params),
    )
    .with_columns(
        macd= pl.col('ema_close_12_wilder') - pl.col('ema_close_26_wilder'),
    )
    #---------------------------------cap
    .with_columns(
        #group 2
        cap_xchgd_approx    = pl.col("Volume") * pl.max_horizontal(pl.col('Open') + pl.col('High')+pl.col('Low')+pl.col('Close')/4, pl.lit(0)),
    )
    #---------------------------------tmf
    .with_columns(
        _close_prev = pl.col('Close').shift(1).over(**over_params)
    )
    .with_columns(
        _trh = pl.max_horizontal(pl.col('High'), pl.col('_close_prev')),
        _trl = pl.min_horizontal(pl.col('Low'), pl.col('_close_prev')),
    )
    .with_columns(
        _ad = (2*pl.col("Close")-pl.col("_trh")-pl.col("_trl"))/(pl.col("_trh")-pl.col("_trl")+0.00000000001)*pl.col("Volume"),
    )
    .with_columns(
        ema_ad = pl.col('_ad').ewm_mean(com=14-1).over(**over_params),
        ema_volume = pl.col('Volume').ewm_mean(com=14-1).over(**over_params),
    )
    .with_columns(
        TMF_w = pl.col('ema_ad')/pl.col('ema_volume')
    )
)
lv2 = (
    lv2.join(
        lv2.rolling(index_column='Date',period=f'{4*5}i',group_by='Ticker').agg(
            pl.min("TMF_w").alias('TMF_4w_min')
        ),
        on = [pl.col("Date"),pl.col("Ticker")]
    ).join(
        lv2.rolling(index_column='Date',period=f'{26*5}i',group_by='Ticker').agg(
            pl.min("TMF_w").alias('TMF_26w_min')
        ),
        on = [pl.col("Date"),pl.col("Ticker")]
    ).with_columns(
        TMF_4w_min_dd  = pl.col("TMF_4w_min")-pl.col("TMF_4w_min").shift(1).over(**over_params) ,
        TMF_26w_min_dd = pl.col("TMF_26w_min")-pl.col("TMF_26w_min").shift(1).over(**over_params),
    )
    .with_columns(
        __TMF_4w_min_dd_qtl_50_neg = pl.when(pl.col("TMF_4w_min_dd")<0).then(pl.col("TMF_4w_min_dd")),
        __TMF_26w_min_dd_qtl_50_neg = pl.when(pl.col("TMF_26w_min_dd")<0).then(pl.col("TMF_26w_min_dd"))
    )
)

lv2 = (
    lv2.join(
        lv2.rolling(index_column='Date',period=f'{4*5}i',group_by='Ticker').agg(
            pl.quantile("__TMF_4w_min_dd_qtl_50_neg", 0.5,interpolation='linear').alias('TMF_4w_min_dd_qtl_50'),
            pl.quantile("__TMF_4w_min_dd_qtl_50_neg", 0.5,interpolation='nearest').alias('TMF_4w_min_dd_qtl_50_alt')
        ),
        on = [pl.col("Date"),pl.col("Ticker")]
    )
    .join(
        lv2.rolling(index_column='Date',period=f'{26*5}i',group_by='Ticker').agg(
            pl.quantile("__TMF_26w_min_dd_qtl_50_neg", 0.5,interpolation='linear').alias('TMF_26w_min_dd_qtl_50'),
            pl.quantile("__TMF_26w_min_dd_qtl_50_neg", 0.5,interpolation='nearest').alias('TMF_26w_min_dd_qtl_50_alt')
        ),
        on = [pl.col("Date"),pl.col("Ticker")]
    )
) 
print('start rsi')

#---------------------------------rsi
lv2 = (
    lv2.with_columns(
        _chg = pl.col('Close')/pl.col('Open')-1,
    )
    .with_columns(
        _avg_gain = pl.when(pl.col('_chg')>0).then(pl.col('_chg')).otherwise(0),
        _avg_loss = pl.when(pl.col('_chg')<=0).then(pl.col('_chg')).otherwise(0),
    )
    .with_columns(
        #rsi = (
        #    100 - 100 / (
        #        1+
        #        pl.col('_avg_gain').mean().over(**over_params).rolling(index_column='Date',period='14d')/
        #        pl.col('_avg_loss').mean().over(**over_params).rolling(index_column='Date',period='14d')
        #    )
        #),
        rsi_ema = (
            100 - 100 / (
                1+
                pl.col('_avg_gain').ewm_mean(com=14-1).over(**over_params)/
                pl.col('_avg_loss').ewm_mean(com=14-1).over(**over_params)
            )
        ),
    )
)
lv2 = (
    lv2.join(
        lv2.rolling(index_column='Date',period='14i',group_by='Ticker').agg(
            pl.mean("_avg_gain").alias('_ma_avg_gain'),
            pl.mean("_avg_loss").alias('_ma_avg_loss')
        ),
        on = [pl.col("Date"),pl.col("Ticker")]
    )
    .with_columns(
        rsi = (
            100 - 100 / (
                1+
                pl.col('_ma_avg_gain')/pl.col('_ma_avg_loss')
            )
        ),
    )
)
# -------------------------------- tmf signal
print('start tmf sig')

lv2 = (
    lv2.with_columns(
        TMF_Simple_Signal = pl.when(
            (pl.col("TMF_26w_min_dd")==0) & (pl.col("TMF_26w_min_dd").shift(1).over(**over_params) < 0)
        ).then(1).otherwise(0)
    )
)

# -------------------------------- ADX
print('start adx')
lv2 = adx(lv2)
#lv2.write_parquet('full_df_debug.pl.parquet')

# -------------------------------- atr
print('start ATR')
lv2 = atr(lv2)


# -------------------------------- supertrend
print('start supertrend')
lv2 = supertrend(lv2, atr_period = 14, multiplier=2)



# -------------------------------- heuristic of tmf signal on simple exit

print('start previous success')

lv2 = (
    lv2.join(
        lv2.rolling(index_column=pl.col('Date'), period=f"{52*5}i", offset = f"{-52*5-20-10}i", group_by="Ticker").agg(
            ((pl.col('exit_gain_loss') > 1.02) & (pl.col("TMF_Simple_Signal")==1)).cast(pl.UInt8).sum().alias("previous_exit_success"),
            ((pl.col('exit_gain_loss') <= 1.02) & (pl.col("TMF_Simple_Signal")==1)).cast(pl.UInt8).sum().alias("previous_exit_fail")
        ).with_columns(
            previous_exit_result_ratio =    pl.col("previous_exit_success")/(pl.col("previous_exit_fail")+1),
        ),
        on = [pl.col("Date"),pl.col("Ticker")]
    )  
    #.with_columns(
    #    previous_exit_result_ratio =    pl.col("previous_exit_success")/(pl.col("previous_exit_fail")+1),
    #)
)

if lazy:
    lv2 = lv2.select(pl.col("*")).collect()

lv2.write_parquet('full_df_debug.pl.parquet')
