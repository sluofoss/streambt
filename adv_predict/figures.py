# artifacts
from sklearn.inspection import permutation_importance
import matplotlib.pyplot as plt

import numpy as np
from numpy.typing import NDArray
import polars as pl

def plot_perm_importance(clf, X, Y, feature_names):
    pr = permutation_importance(clf, X, Y, n_repeats=30, random_state=0)
    perm_sorted_idx = pr.importances_mean.argsort()[::-1]
    style = 'fast'
    plot_size = (10,8)
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        ax.barh(feature_names,pr.importances_mean[perm_sorted_idx], xerr=pr.importances_std[perm_sorted_idx])
        ax.axhline(y=0, color="red", linestyle="--")
        ax.set_title("Permutation Importance of Features", fontsize=14)
        ax.set_xlabel("Permutation Importance", fontsize=12)
        ax.set_ylabel("Features", fontsize=12)
        plt.tight_layout()
    plt.close(fig)
    return fig


def plot_yearly_return(
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
    agg = (
        df.filter(pl.col('predict')==True).group_by(pl.col('date').dt.month_start().alias('date')).agg( 
            pl.col('true_pct').mean().alias('monthly_return_average'),
            pl.col('true_pct').std(ddof=0).alias('monthly_return_std'),
            (pl.col('true_pct').mean()**2+pl.col('true_pct').var(ddof=0)).alias('monthly_e_x_square'),
            pl.col('true_pct').min().alias('monthly_min_trade'),
            pl.col('true_pct').max().alias('monthly_max_trade'),
        )
        .group_by(pl.col('date').dt.year().alias('date')).agg(
            (pl.col('monthly_return_average').product().alias('yearly_exponential_return')),
            (pl.col('monthly_e_x_square').product()-pl.col('monthly_return_average').product()**2).sqrt().alias("yearly_std"),
            #pl.col('monthly_return_average').min().alias('yearly_min_month'),
            #pl.col('monthly_return_average').max().alias('yearly_max_month'),
            # asked deepseek derive the variance/standard deviation of product of normal distributions. i.e. x1*x2*x3*...*xn where x_i ~ Normal(mu_i,sigmal_i)
        )
        .sort('date').to_pandas()
    )
    style = 'ggplot'
    plot_size = (10,8)
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        ax.errorbar(agg.date, agg.yearly_exponential_return, yerr = agg.yearly_std, fmt='-o', capsize=5, label="Monthly Average Return")
        #ax.bar(agg.date, agg.monthly_return_average, yerr = agg.monthly_return_std)
        #ax.axhline(y=0, color="red", linestyle="--")
        ax.set_title("Yearly Exponential Return", fontsize=14)
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Return", fontsize=12)
        plt.tight_layout()
    plt.close(fig)
    return fig
def plot_yearly_return_table(
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
    agg = (
        df.filter(pl.col('predict')==True).group_by(pl.col('date').dt.month_start().alias('date')).agg( 
            pl.col('true_pct').mean().alias('monthly_return_average'),
            pl.col('true_pct').std(ddof=0).alias('monthly_return_std'),
            (pl.col('true_pct').mean()**2+pl.col('true_pct').var(ddof=0)).alias('monthly_e_x_square'),
            pl.col('true_pct').min().alias('monthly_min_trade'),
            pl.col('true_pct').max().alias('monthly_max_trade'),
        )
        .group_by(pl.col('date').dt.year().alias('date')).agg(
            (pl.col('monthly_return_average').product().alias('yearly_exponential_return')),
            (pl.col('monthly_e_x_square').product()-pl.col('monthly_return_average').product()**2).sqrt().alias("yearly_std"),
            #pl.col('monthly_return_average').min().alias('yearly_min_month'),
            #pl.col('monthly_return_average').max().alias('yearly_max_month'),
            # asked deepseek derive the variance/standard deviation of product of normal distributions. i.e. x1*x2*x3*...*xn where x_i ~ Normal(mu_i,sigmal_i)
        )
        .sort('date').to_pandas()
    )
    style = 'ggplot'
    plot_size = (10,8)
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        if not agg.empty:
            ax.table(cellText=agg.values, colLabels=agg.columns, loc="center")
            ax.set_title("Monthly Average Return", fontsize=14)
        else:
            ax.text(
                0.5,
                0.5,
                "No data available",
                fontsize=12,
                ha="center",
                va="center",
            )
        
        plt.tight_layout()
    plt.close(fig)
    return fig


def plot_monthly_return(
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
    agg = df.filter(pl.col('predict')==True).group_by(pl.col('date').dt.month_start().alias('date')).agg( 
        pl.col('true_pct').mean().alias('monthly_return_average'),
        pl.col('true_pct').std(ddof=0).alias('monthly_return_std'),
        pl.col('true_pct').min().alias('monthly_min_trade'),
        pl.col('true_pct').max().alias('monthly_max_trade'),

    ).sort('date').to_pandas()
    style = 'ggplot'
    plot_size = (10,8)
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        ax.errorbar(agg.date, agg.monthly_return_average, yerr = agg.monthly_return_std, fmt='-o', capsize=5, label="Monthly Average Return")
        ax.errorbar(
            agg.date, 
            agg.monthly_return_average, 
            yerr = [
                agg.monthly_return_average - agg.monthly_min_trade,
                agg.monthly_max_trade - agg.monthly_return_average,
            ], 
            fmt='-o', capsize=5, label="Monthly Average Return"
        )
        #ax.bar(agg.date, agg.monthly_return_average, yerr = agg.monthly_return_std)
        #ax.axhline(y=0, color="red", linestyle="--")
        ax.set_title("Monthly Average Return", fontsize=14)
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Return", fontsize=12)
        plt.tight_layout()
    plt.close(fig)
    return fig

def plot_monthly_return_table(
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
    agg = df.filter(pl.col('predict')==True).group_by(pl.col('date').dt.month_start().alias('date')).agg( 
        pl.col('true_pct').mean().alias('monthly_return_average'),
        pl.col('true_pct').std(ddof=0).alias('monthly_return_std'),

    ).sort('date').to_pandas()
    style = 'fast'
    plot_size = (10,(len(agg.date)+1)/4)
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        if not agg.empty:
            ax.table(cellText=agg.values, colLabels=agg.columns, loc="center")
            ax.set_title("Monthly Average Return", fontsize=14)
        else:
            ax.text(
                0.5,
                0.5,
                "No data available",
                fontsize=12,
                ha="center",
                va="center",
            )
        
        plt.tight_layout()
    plt.close(fig)
    return fig