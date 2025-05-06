import numpy as np
from numpy.typing import NDArray
from sklearn.metrics import precision_score
import polars as pl

from sklearn.metrics import confusion_matrix

def precision_at(y_true:NDArray[np.float64], y_pred:NDArray[np.float64], threshold:float):
    """
    choice of threshold depends on the regression dataset
    """
    # TODO:
    # should this be changed?
    # e.g. when  predict > 1.x and true > 1, should this still technically be considered a win?
    # i.e. should there be 2 thresholds? 1 for predict and 1 for true. 
    #
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

def true_positive_at(y_true:NDArray[np.float64], y_pred:NDArray[np.float64], threshold:float):
    y_t_alt = (y_true > threshold)
    y_p_alt = (y_pred > threshold)
    cm = confusion_matrix(y_t_alt, y_p_alt)
    # Extract TP, TN, FP, FN
    tn, fp, fn, tp = cm.ravel()
    return float(tp)


def mthly_rtrn_pct_avg(
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
        "predict": y_p_alt.astype(bool),
        'true_pct': y_true,
    })
    #print(df)
    agg = df.filter(pl.col('predict')==True).group_by(pl.col('date').dt.month_start()).agg( 
        pl.col('true_pct').mean()
    )
    #print(agg)
    #print(agg.mean())
    res = agg.select('true_pct').mean().item()
    if agg['true_pct'].len() == 0:
        return 1
    if res is None:
        print('debug pct avg:')
        print(agg, res)
    return res

def mthly_rtrn_pct_std(
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
        "predict": y_p_alt.astype(bool),
        'true_pct': y_true,
    })
    #print(df)
    agg = df.filter(pl.col('predict')==True).group_by(pl.col('date').dt.month_start()).agg( 
        pl.col('true_pct').mean()
    )
    res = agg.select('true_pct').std(ddof=0).item()
    if agg['true_pct'].len() == 0:
        return 0
    if res is None:
        print('debug pct std:')
        print(agg, res)
    return res

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
    if agg['TP'].len() == 0: # tp should be relabeled as precision here
        return 0
    res = agg.mean().item()
    if res is None:
        print('debug win avg:')
        print(agg, res)
    return res

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
    # TODO URGENT!!!:
    # this metric and probably the others as well breaks with following output
    """
      _warn_prf(average, modifier, f"{metric.capitalize()} is", len(result))
debug win std:
shape: (1, 1)
┌─────┐
│ TP  │
│ --- │
│ f64 │
╞═════╡
│ 1.0 │
└─────┘ None
monthly_win_ratio_std threshold_1.01 None"""
    # need to break the above agg step into multiple steps and inspect details 
    
    #print(agg)
    if agg['TP'].len() == 0: # tp should be relabeled as precision here
        return 0
    res = agg.std(ddof=0).item()
    if res is None:
        print('debug win std:')
        print(agg, res)
    return res

import functools as ft
def named_partial(func, *args, **kwargs):
    f = ft.partial(func, *args, **kwargs) 
    #f.__name__ = f"{f.func.__name__}({",".join(list(args)+[f"{k}={v}" for k,v in kwargs.items()])})"
    f.__name__ = f"{f.func.__name__}__{"__".join(list(args)+[f"{k}_{v}" for k,v in kwargs.items()])}"
    # WARNING: for parameter 'name' supplied: Names may only contain alphanumerics, underscores (_), dashes (-), periods (.), spaces ( ), colon(:) and slashes (/).
    
    # TODO decide whether it should include unfilled partial arguments here 
    return f
