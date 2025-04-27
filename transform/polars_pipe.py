import polars as pl
import polars.datatypes as DTypes
print('start script')
def exit_signal_statistics_ts_by_ticker(
        df:pl.DataFrame, 
        entry_price_col:pl.Expr=(pl.col('Open')+pl.col('High')+pl.col('Low')+pl.col('Close'))/4,
        exit_sig_col:pl.Expr=pl.col('Close'),
        # TODO define slipperage clearer
        max_hold_period = 14,
        period_unit = 'i',
        max_gain = 1.05, 
        max_loss = 0.95,
        window = None # this needs to be fixed
    ) -> pl.DataFrame: 
    """
    entry_price: assumed to be for as if the order is placed at the start of the current ts. i.e. the buy signal would be from 1 step previous
    # should one assume this to be from the current step instead??
    """
    tmp = (
        df
        .rolling(index_column='Date',period=f'{max_hold_period+2}{period_unit}',group_by='Ticker')
        .agg(
            exit_sig_col.alias('exit_price_signal_list'),
            entry_price_col.alias('exit_fluctuated_price_list'),
        )
        #.with_columns(
        #    exit_price_signal_list    = pl.when(pl.col("exit_price_signal_list").list.len()==(max_hold_period+2)).then(pl.col("exit_price_signal_list")).otherwise(None),
        #    exit_fluctuated_price_list = pl.when(pl.col("exit_fluctuated_price_list").list.len()==(max_hold_period+2)).then(pl.col("exit_fluctuated_price_list")).otherwise(None)
        #)
    )
    tmp = tmp.with_columns(
        # price which will trigger signal
        look_forward_signal_price = pl.col('exit_price_signal_list').shift(-max_hold_period-2).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")),
        # price which transaction will be assumed to happen on
        look_forward_exit_price = pl.col('exit_fluctuated_price_list').shift(-max_hold_period-2).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")),
    )

    tmp = (
        df.join(
            tmp,
            on=[pl.col('Date'),pl.col('Ticker')]
        ).with_columns(
            entry_price = pl.col("look_forward_exit_price").list.get(0)
        ).with_columns(
            change_since_entry = pl.col('look_forward_signal_price')/pl.col("entry_price")
        ).with_columns(
            over_gain_signal = pl.col('change_since_entry').list.eval(
                    pl.when(pl.element()>max_gain).then(1)
                ).list.arg_max(),
            over_loss_signal = pl.col('change_since_entry').list.eval(
                    pl.when(pl.element()<max_loss).then(1)
                    #(pl.element()<max_loss).cast(pl.Int64)
                ).list.arg_max(),
            over_period_signal = pl.col('look_forward_signal_price').list.len()-2,#max_hold_period-1,
        ).with_columns(
            exit_signal = pl.min_horizontal("over_gain_signal","over_loss_signal","over_period_signal"),
            exit_point = pl.min_horizontal("over_gain_signal","over_loss_signal","over_period_signal")+1
        )
        .with_columns(
            exit_price = pl.col("look_forward_exit_price").list.get(pl.col("exit_point")),
            
            exit_gain_loss = pl.col("look_forward_exit_price").list.get(pl.col("exit_point"))/pl.col("entry_price")
        )
        .drop([
            "exit_price_signal_list",
            "exit_fluctuated_price_list",
            "look_forward_signal_price",
            "look_forward_exit_price",
            "change_since_entry",
            "over_gain_signal",
            "over_loss_signal",
            "over_period_signal",
        ])
    )
    
    #.with_columns(
    #    over_gain_signal_1 = pl.col('over_gain_signals').list.arg_max(),
    #    over_loss_signal_1 = pl.col('over_loss_signals').list.arg_max()
    #    
    #)

    return tmp
def adx(df:pl.DataFrame):
    #https://github.com/twopirllc/pandas-ta/blob/main/pandas_ta/trend/adx.py
    over_params = {'partition_by':'Ticker', 'order_by':"Date"}
    base = (
        df.with_columns(
            # https://www.investopedia.com/articles/trading/07/adx-trend-indicator.asp
            # Plus directional movement (+DM) = Current High - Previous High
            # Minus directional movement (-DM) = Previous Low - Current Low 
            up = pl.col('High') - pl.col('High').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")),
            dn = pl.col('Low').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")) - pl.col('Low'),
        )
        .with_columns(
            PDM = pl.when( (pl.col('up')>pl.col('dn')) & (pl.col('up')>0) ).then(pl.col('up')).otherwise(0), 
            NDM = pl.when( (pl.col('dn')>pl.col('up')) & (pl.col('dn')>0) ).then(pl.col('dn')).otherwise(0),
        )
        .with_columns(
            TR = pl.max_horizontal(
                (pl.col('High') - pl.col('Low')),
                (pl.col('High') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
                (pl.col('Low') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
            )
        )
    )
    tmp = (
        base.join(
            base.rolling(index_column='Date',period=f'{14}i',group_by='Ticker').agg(
                pl.col('TR').mean().alias('ATR'),
            ),
            on=[pl.col('Date'),pl.col('Ticker')]
        )
        .with_columns(
            PDI = pl.col('PDM').ewm_mean(com=14-1).over(**over_params)/pl.col('ATR'),
            NDI = pl.col('NDM').ewm_mean(com=14-1).over(**over_params)/pl.col('ATR')
        )
        .with_columns(
            DX = (pl.col('PDI')-pl.col('NDI')).abs()/(pl.col('PDI')+pl.col('NDI')).abs()
        )
        .with_columns(
            ADX = pl.col('DX').fill_nan(0).ewm_mean(com=14-1).over(**over_params)
        )
        #.drop([
        #    'PDM',
        #    'NDM',
        #    'TR',
        #    'ATR',
        #    'DX',
        #])
    )

    return tmp

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
lv2.write_parquet('full_df_debug.pl.parquet')
# -------------------------------- entry_exit_calc

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


lv3 = exit_signal_statistics_ts_by_ticker(lv2,max_hold_period=10)

print('start previous success')

lv4 = (
    lv3.join(
        lv3.rolling(index_column=pl.col('Date'), period=f"{52*5}i", offset = f"{-52*5-20-10}i", group_by="Ticker").agg(
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
    lv4 = lv4.select(pl.col("*")).collect()
lv4.write_parquet('full_df_debug.pl.parquet')

