import polars as pl
import inspect

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
    f_name = inspect.currentframe().f_code.co_name
    print(f'{f_name}: finished list')
    tmp = tmp.with_columns(
        # price which will trigger signal
        look_forward_signal_price = pl.col('exit_price_signal_list').shift(-max_hold_period-2).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")),
        # price which transaction will be assumed to happen on
        look_forward_exit_price = pl.col('exit_fluctuated_price_list').shift(-max_hold_period-2).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")),
    )
    print(f'{f_name}: finished look_forward')

    tmp = (
        df.join(
            tmp,
            on=[pl.col('Date'),pl.col('Ticker')]
        )
    )

    print(f'{f_name}: finished join')

    tmp = (
        tmp.with_columns(
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
    )

    print(f'{f_name}: finished signal')

    tmp = (
        tmp
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
    print(f'{f_name}: finished all')

    return tmp

def atr(df, period = 14):
    tmp = (
        df.with_columns(
            _TR = pl.max_horizontal(
                (pl.col('High') - pl.col('Low')),
                (pl.col('High') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
                (pl.col('Low') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
            )
        )
    )
    return tmp.join(
        tmp.rolling(index_column='Date',period=f'{period}i',group_by='Ticker').agg(
            pl.col('_TR').mean().alias('ATR'), #TODO refactor ATR out
        ),
        on=[pl.col('Date'),pl.col('Ticker')]
    ) 

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
            _TR = pl.max_horizontal(
                (pl.col('High') - pl.col('Low')),
                (pl.col('High') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
                (pl.col('Low') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
            )
        )
    )
    tmp = (
        base.join(
            base.rolling(index_column='Date',period=f'{14}i',group_by='Ticker').agg(
                pl.col('_TR').mean().alias('_ATR'), #TODO refactor ATR out
            ),
            on=[pl.col('Date'),pl.col('Ticker')]
        )
        .with_columns(
            PDI = pl.col('PDM').ewm_mean(com=14-1).over(**over_params)/pl.col('_ATR'),
            NDI = pl.col('NDM').ewm_mean(com=14-1).over(**over_params)/pl.col('_ATR')
        )
        .with_columns(
            DX = (pl.col('PDI')-pl.col('NDI')).abs()/(pl.col('PDI')+pl.col('NDI')).abs()
        )
        .with_columns(
            ADX = pl.col('DX').fill_nan(0).ewm_mean(com=14-1).over(**over_params)
        )
        .drop([
            'PDM',
            'NDM',
            '_TR',
            '_ATR',
            'DX',
        ])
    )

    return tmp
def normalized_average_true_range():
    # antr
    # https://www.macroption.com/normalized-atr/
    raise NotImplementedError(f"function {inspect.currentframe().f_code.co_name} not implemented")
    
def average_normalized_true_range():
    # antr
    # https://www.macroption.com/normalized-atr/
    raise NotImplementedError(f"function {inspect.currentframe().f_code.co_name} not implemented")

def supertrend(df:pl.DataFrame, atr_period=14, multiplier=3):
    # since supertrend is not normalized and it is a very simple tweak of atr, perhaps something more useful would be normalized atr?
    over_params = {'partition_by':'Ticker', 'order_by':"Date"}
    tmp = (
        df.with_columns(
            _TR = pl.max_horizontal(
                (pl.col('High') - pl.col('Low')),
                (pl.col('High') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
                (pl.col('Low') - pl.col('Close').shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))).abs(),
            )
        )
    ) 
    tmp = (
        tmp.join(
            tmp.rolling(index_column='Date',period=f'{atr_period}i',group_by='Ticker').agg(
                pl.col('_TR').mean().alias('_ATR'), #TODO refactor ATR out
            ),
            on=[pl.col('Date'),pl.col('Ticker')]
        )
        .with_columns(
            st_upper = ( (pl.col('High')+pl.col('Low'))/2 + multiplier*pl.col('_ATR') )
                .shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date")),
            st_lower = ( (pl.col('High')+pl.col('Low'))/2 - multiplier*pl.col('_ATR') )
                .shift(1).over(partition_by=pl.col('Ticker'),order_by=pl.col("Date"))
        )
        .drop([
            '_TR',
            '_ATR',
        ])
    )
    return tmp 