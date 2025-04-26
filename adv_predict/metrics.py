import numpy as np
from numpy.typing import NDArray
from sklearn.metrics import precision_score
import polars as pl

def precision_at(y_true:NDArray[np.float64], y_pred:NDArray[np.float64], threshold:float):
    """
    choice of threshold depends on the regression dataset
    """
    y_t_alt = (y_true > threshold)
    y_p_alt = (y_pred > threshold)
    #print('---------------------')
    #print('---------------------')
    #print('---------------------')
    #print('---------------------')
    #print('---------------------')
    #print(y_t_alt)
    #print('---------------------')

    return precision_score(y_t_alt, y_p_alt)

def monthly_win_ratio_avg(
    y_true:NDArray[np.float64], 
    y_pred:NDArray[np.float64], 
    threshold:float, 
    date:NDArray[np.datetime64]
)->float:
    y_t_alt = (y_true > threshold)
    y_p_alt = (y_pred > threshold)
    #print(np.stack([date, y_t_alt, y_p_alt],axis=1, dtype=object).shape)
    #df = pl.from_numpy(np.stack([date, y_t_alt, y_p_alt],axis=1, dtype=object), schema=[("date",pl.Datetime), ("true",pl.Boolean), ('predict',pl.Boolean)], orient="row")
    df = pl.DataFrame({
        "date": date.astype('datetime64[ms]'),  # Ensure datetime format
        "true": y_t_alt.astype(bool),
        "predict": y_p_alt.astype(bool)
    })
    #print(df)
    agg = df.group_by(pl.col('date').dt.month_start()).agg( 
        ((pl.col('true')==pl.col('predict')) & (pl.col('true')==True)).cast(pl.Int32).sum().alias('TP'), 
        ((pl.col('true')!=pl.col('predict')) & (pl.col('true')==False)).cast(pl.Int32).sum().alias('FP'), 
    )
    #print(agg)
    agg = agg.select(pl.col('TP')/(pl.col('TP')+pl.col('FP'))).drop_nans()
    #print(agg)
    return agg.mean().item()

def monthly_win_ratio_std(
    y_true:NDArray[np.float64], 
    y_pred:NDArray[np.float64], 
    threshold:float, 
    date:NDArray[np.datetime64]
)->float:
    y_t_alt = (y_true > threshold)
    y_p_alt = (y_pred > threshold)
    df = pl.DataFrame({
        "date": date.astype('datetime64[ms]'),  # Ensure datetime format
        "true": y_t_alt.astype(bool),
        "predict": y_p_alt.astype(bool)
    })
    agg = df.group_by(pl.col('date').dt.month_start()).agg( 
        ((pl.col('true')==pl.col('predict')) & (pl.col('true')==True)).cast(pl.Int32).sum().alias('TP'), 
        ((pl.col('true')!=pl.col('predict')) & (pl.col('true')==False)).cast(pl.Int32).sum().alias('FP'), 
    ).select(pl.col('TP')/(pl.col('TP')+pl.col('FP'))).drop_nans()
    #print(agg)
    return agg.std().item()

import functools as ft
def named_partial(func, *args, **kwargs):
    f = ft.partial(func, *args, **kwargs) 
    #f.__name__ = f"{f.func.__name__}({",".join(list(args)+[f"{k}={v}" for k,v in kwargs.items()])})"
    f.__name__ = f"{f.func.__name__}-{",".join(list(args)+[f"{k}:{v}" for k,v in kwargs.items()])}"
    # TODO decide whether it should include unfilled partial arguments here 
    return f
