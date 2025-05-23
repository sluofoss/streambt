# load data
import duckdb as db
import polars as pl
from pathlib import Path

# param trial
import optuna
from optuna.samplers import TPESampler

# model defintion
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor

# metrics 
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_poisson_deviance,
    mean_gamma_deviance,
    mean_tweedie_deviance,
)

from metrics import precision_at, true_positive_at, named_partial 
from metrics import (
    monthly_win_ratio_avg, 
    monthly_win_ratio_std,
    mthly_rtrn_pct_avg,
    mthly_rtrn_pct_std,
)


from inspect import signature, Parameter



# track parameter and result
import mlflow
from mlflow.models import infer_signature


# artifacts
from figures import plot_perm_importance, plot_monthly_return, plot_monthly_return_table, plot_yearly_return, plot_yearly_return_table



mlflow.set_tracking_uri('http://localhost:5000')

parquet_file = Path("/home/sean/Projects/streambt/full_df_debug.pl.parquet")
#full_df = pd.read_parquet(parquet_file).rename({'Date':'Date_index', 'Date_str':'Date'}, axis = 'columns')
full_df = pl.read_parquet(parquet_file).rename({'Date':'Date_index', 'Date_str':'Date'})

#temp model store (hack for small models)
#https://stackoverflow.com/questions/73539873/saving-trained-models-in-optuna-to-a-variable
from collections import defaultdict
models = defaultdict(dict)



# define evaluation metrics (could be tightly coupled with dataset)
# order by the ones to use during optimization (TODO pareto front)
eval_metrics = [
    named_partial(precision_at, threshold = 1.01),
    named_partial(precision_at, threshold = 1.05),
    named_partial(precision_at, threshold = 1.10),
    
    named_partial(true_positive_at, threshold = 1.01),
    named_partial(true_positive_at, threshold = 1.05),
    named_partial(true_positive_at, threshold = 1.10),

    named_partial(monthly_win_ratio_avg, threshold = 1.01),
    named_partial(monthly_win_ratio_avg, threshold = 1.05),
    named_partial(monthly_win_ratio_avg, threshold = 1.10),

    named_partial(monthly_win_ratio_std, threshold = 1.01),
    named_partial(monthly_win_ratio_std, threshold = 1.05),
    named_partial(monthly_win_ratio_std, threshold = 1.10),

    named_partial(mthly_rtrn_pct_avg, threshold = 1.01),
    named_partial(mthly_rtrn_pct_avg, threshold = 1.05),
    named_partial(mthly_rtrn_pct_avg, threshold = 1.10),

    named_partial(mthly_rtrn_pct_std, threshold = 1.01),
    named_partial(mthly_rtrn_pct_std, threshold = 1.05),
    named_partial(mthly_rtrn_pct_std, threshold = 1.10),

    
    #named_partial(precision_at ),
    mean_absolute_error,
    mean_squared_error,
    mean_poisson_deviance,
    mean_gamma_deviance,
    named_partial(mean_tweedie_deviance, power=3),
]

def evaluate_objective(available_args):
    res = {}
    #print('-------------')
    for func in eval_metrics:
        sig = signature(func)
        params = sig.parameters
        
        # Collect required and optional parameters
        kwargs = {}
        for param_name, param in params.items():
            if param_name in available_args:
                kwargs[param_name] = available_args[param_name]
            elif param.default == Parameter.empty:
                # Handle missing required parameters (if any)
                raise ValueError(f"Missing required parameter '{param_name}' for {func.__name__}")
        # Call the function with the collected arguments
        #print(func.__name__)
        #print(kwargs.keys())
        try:
            metric_value = func(**kwargs)
        except Exception as e:
            print(f"Error evaluating {func.__name__}: {e}")
            raise e
            #continue
        #print(func.__name__,metric_value)
        if metric_value is None:
            print(func.__name__,metric_value)
            print(available_args)
        # Store and log the result
        res[f"{func.__name__}"] = metric_value
    return res

def objective(trial:optuna.Trial):
    # suggest hyperparam and model definition
    regressor_type  = trial.suggest_categorical('regressor_type',(
        'DecisionTreeRegressor',
        'RandomForestRegressor',
        'HistGradientBoostingRegressor'
    ))
    base_params = {'regressor_type':regressor_type}
    if regressor_type == 'RandomForestRegressor':
        sub_params = {
            'n_estimators'    : trial.suggest_int("n_estimators", 1, 10, step = 1),
            'max_depth'       : trial.suggest_int("max_depth", 2, 10, step = 1),
            'criterion'       : trial.suggest_categorical("criterion", ['squared_error', 'friedman_mse', 'absolute_error', 'poisson']),
        }
        clf = RandomForestRegressor(**sub_params)
    elif regressor_type == 'HistGradientBoostingRegressor':
        sub_params = {
            'max_iter'    : trial.suggest_int("max_iter", 1, 10, step = 1),
            'max_depth'   : trial.suggest_int("max_depth", 2, 10, step = 1),
            'loss'        : trial.suggest_categorical("loss", ['squared_error', 'absolute_error', 'gamma', 'poisson']), #'quantile' need further subgrid definition
        }
        clf = HistGradientBoostingRegressor(**sub_params)
    
    elif regressor_type == 'DecisionTreeRegressor':
        sub_params = {
            'max_depth'   : trial.suggest_int("max_depth", 2, 10, step = 1),
            'criterion'       : trial.suggest_categorical("criterion", ['squared_error', 'friedman_mse', 'absolute_error', 'poisson']),
        }
        clf = DecisionTreeRegressor(**sub_params)
    else:
        raise Exception('unknown regressor type')
    params = {**base_params, **sub_params}
    print('--logging params--')
    print(params)
    # data_load
    feature_target = db.sql(
    f"""
    with extra as (
        select 
        *
        , sum(cap_xchgd_approx)                over (partition by Ticker order by Date rows between 5*52 preceding and current row) as yearly_cap_xchngd
        
        , stddev_samp(log(Volume+1))                    over (partition by Ticker order by Date rows between 5*52 preceding and current row) as std_vol
        , log(Volume + 1)                               as log_vol
        
        , stddev_samp(log(cap_xchgd_approx+100))        over (partition by Ticker order by Date rows between 5*52 preceding and current row) as std_cap
        , log(cap_xchgd_approx + 100)                   as log_cap

        from full_df
        
    )
    select 
        TMF_w                           ::DOUBLE
        , TMF_4w_min                    ::DOUBLE
        , TMF_4w_min_dd                 ::DOUBLE
        , TMF_26w_min                   ::DOUBLE
        , TMF_26w_min_dd                ::DOUBLE
        , TMF_4w_min_dd_qtl_50          ::DOUBLE
        , TMF_4w_min_dd_qtl_50_alt      ::DOUBLE
        , TMF_26w_min_dd_qtl_50         ::DOUBLE
        , TMF_26w_min_dd_qtl_50_alt     ::DOUBLE
        
        , previous_exit_success         ::DOUBLE
        --, previous_exit_success2
        , previous_exit_result_ratio    ::DOUBLE
        --, previous_exit_result_ratio2
        --, TMF_Simple_Signal
        --, macd_hof
        --, macd_signal_hof
        --, macd_w_hof
        --, macd_w_signal_hof
        --, Volume
        --, RSI
        --, RSI_ema
        , PDI
        , NDI
        , ADX

        , st_up_diff_norm
        , st_lo_diff_norm
 
        --, yearly_cap_xchngd
        , log(yearly_cap_xchngd+100) as log_year_cap
        , std_vol
        , log_vol
        , std_cap
        , log_cap
        
        , cast(dayofweek(Date) as DOUBLE)               as day_of_week
        , Date
        , Ticker
        , exit_gain_loss::DOUBLE                            as exit_gain_loss             
        , exit_gain_loss::DOUBLE                            as target
    from extra
    where TMF_Simple_Signal = 1
    --and Ticker in string 
    """
    )
    train       = db.sql("select * from feature_target where year(Date) in (2005,2007,2009,2011,2013,2015)").df().dropna() # worked ????
    validate    = db.sql("select * from feature_target where year(Date) in (2017,2018,2019,2020,2021,2022)").df().dropna() # worked ????
    test        = db.sql("select * from feature_target where year(Date) in (2023,2024)").df().dropna()
    drop_columns = ['Date', 'Ticker', 'target', 'exit_gain_loss']
    # categories: metadata, eval_metric_related, target
    
    #print(train.max(axis=0))
    #TODO: debug cause for unbounded/extreme values for pdi,ndi,st_up/dn_diff_norm

    X_train, Y_train = train.drop(columns = drop_columns), train['target']
    X_validate, Y_validate = validate.drop(columns = drop_columns), validate['target']
    X_test, Y_test = test.drop(columns = drop_columns), test['target']
    
    with mlflow.start_run(nested=True):
        # Log param
        # log early to detect failure
        mlflow.log_params(params)
        
        # model_train
        clf.fit(X_train,Y_train)

        # model eval
        Y_validate_predict = clf.predict(X_validate)
        Y_test_predict = clf.predict(X_test)

        # Log results
        available_args = {
            'y_true': Y_validate,
            'y_pred': Y_validate_predict,
            'date': validate['Date'],  # Ensure this is the correct key for dates
        }
        res = evaluate_objective(available_args)
        for metric_name, metric_value in res.items():
            mlflow.log_metric(f"vldt_{metric_name}", metric_value)

        # TODO: currently this is for temporarily no better way of refactor hyperparameter tuning on dataset parameters 
        # (the formation of features is dependent between samples in train vldt and test due to time series)
        available_args = {
            'y_true': Y_test,
            'y_pred': Y_test_predict,
            'date': test['Date'],  # Ensure this is the correct key for dates
        }
        test_res = evaluate_objective(available_args)
        for metric_name, metric_value in test_res.items():
            mlflow.log_metric(f"test__{metric_name}", metric_value)

        #for func in eval_metrics:
        #    res[f"{func.__name__}"] = func(Y_validate, Y_validate_predict)
        #    mlflow.log_metric(f"{func.__name__}", func(Y_validate, Y_validate_predict))
        
        sig = infer_signature(X_train, Y_train) 

        models[trial.study.study_name][trial.number] = clf
        # Log model
        mlflow.sklearn.log_model(clf, "model", signature=sig)

        # Log additional artifacts (plots, text, matrices)

        # takes too long, consider subsets?
        # mlflow.shap.log_explanation(clf.predict, X_train)

        mlflow.log_figure(plot_perm_importance(clf, X_validate, Y_validate, X_train.columns), 'perm_importance.png')
        
        mlflow.log_figure(plot_monthly_return(Y_validate, Y_validate_predict, 1.05, validate['Date']), 'vldt_monthly_return.png')
        mlflow.log_figure(plot_monthly_return_table(Y_validate, Y_validate_predict, 1.05, validate['Date']), 'vldt_monthly_return_table.png')
        mlflow.log_figure(plot_monthly_return(Y_test, Y_test_predict, 1.05, test['Date']), 'test_monthly_return.png')
        mlflow.log_figure(plot_monthly_return_table(Y_test, Y_test_predict, 1.05, test['Date']), 'test_monthly_return_table.png')

        mlflow.log_figure(plot_yearly_return(Y_validate, Y_validate_predict, 1.05, validate['Date']), 'vldt_yearly_return.png')
        mlflow.log_figure(plot_yearly_return_table(Y_validate, Y_validate_predict, 1.05, validate['Date']), 'vldt_yearly_return_table.png')
        mlflow.log_figure(plot_yearly_return(Y_test, Y_test_predict, 1.05, test['Date']), 'test_yearly_return.png')
        mlflow.log_figure(plot_yearly_return_table(Y_test, Y_test_predict, 1.05, test['Date']), 'test_yearly_return_table.png')        


    return res[eval_metrics[0].__name__]
    #return res['mean_poisson_deviance']#{"loss": eval_rmse, "status": STATUS_OK, "model": model}

# 3. Create a study object and optimize the objective function.
mlflow.set_experiment("/stock_signal_predict")
with mlflow.start_run():
    study = optuna.create_study(direction='maximize', sampler=TPESampler())
    study.optimize(objective, n_trials=100)
    
    mlflow.log_params(study.best_params)
    #mlflow.log_metric("mean_poisson_deviance", study.best_value)
    mlflow.log_metric(eval_metrics[0].__name__, study.best_value)


    mlflow.sklearn.log_model(models[study.study_name][study.best_trial.number], "model")
    
    