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

# track parameter and result
import mlflow
from mlflow.models import infer_signature
mlflow.set_tracking_uri('http://localhost:5000')

parquet_file = Path("/home/sean/Projects/streambt/full_df_debug.pl.parquet")
#full_df = pd.read_parquet(parquet_file).rename({'Date':'Date_index', 'Date_str':'Date'}, axis = 'columns')
full_df = pl.read_parquet(parquet_file).rename({'Date':'Date_index', 'Date_str':'Date'})

#temp model store (hack for small models)
#https://stackoverflow.com/questions/73539873/saving-trained-models-in-optuna-to-a-variable
from collections import defaultdict
models = defaultdict(dict)

def objective(trial:optuna.Trial):
    # suggest hyperparam and model definition
    regressor_type  = trial.suggest_categorical('regressor_type',(
        'DecisionTreeRegressor',
        'RandomForestRegressor',
        'HistGradientBoostingRegressor',
    ))
    base_params = {'model_type':regressor_type}
    if regressor_type == 'RandomForestRegressor':
        sub_params = {
            'n_estimators'    : trial.suggest_int("n_estimators", 1, 10, 1),
            'max_depth'       : trial.suggest_int("max_depth", 2, 10, 1),
            'criterion'       : trial.suggest_categorical("criterion", ['squared_error', 'friedman_mse', 'absolute_error', 'poisson']),
        }
        clf = RandomForestRegressor(**sub_params)
    elif regressor_type == 'HistGradientBoostingRegressor':
        sub_params = {
            'max_iter'    : trial.suggest_int("max_iter", 1, 10, 1),
            'max_depth'   : trial.suggest_int("max_depth", 2, 10, 1),
            'loss'        : trial.suggest_categorical("loss", ['squared_error', 'absolute_error', 'gamma', 'poisson']), #'quantile' need further subgrid definition
        }
        clf = HistGradientBoostingRegressor(**sub_params)
    
    elif regressor_type == 'DecisionTreeRegressor':
        sub_params = {
            'max_depth'   : trial.suggest_int("max_depth", 2, 10, 1),
            'criterion'       : trial.suggest_categorical("criterion", ['squared_error', 'friedman_mse', 'absolute_error', 'poisson']),
        }
        clf = DecisionTreeRegressor(**sub_params)
    params = {**base_params, **sub_params}
    print('--logging params--')
    print(params)
    # data_load
    feature_target = db.sql(
    f"""
    select 
        TMF_w
        , TMF_4w_min
        , TMF_4w_min_dd
        , TMF_26w_min
        , TMF_26w_min_dd
        , TMF_4w_min_dd_qtl_50
        , TMF_4w_min_dd_qtl_50_alt
        , TMF_26w_min_dd_qtl_50
        , TMF_26w_min_dd_qtl_50_alt 
        
        , previous_exit_success
        --, previous_exit_success2
        , previous_exit_result_ratio
        --, previous_exit_result_ratio2
        --, TMF_Simple_Signal
        --, macd_hof
        --, macd_signal_hof
        --, macd_w_hof
        --, macd_w_signal_hof
        --, Volume
        --, RSI
        --, RSI_ema
        , stddev_samp(log(Volume+1)) over (partition by Ticker order by Date rows between 5*52 preceding and current row) as std_vol
        , log(Volume + 1)  as log_vol
        , stddev_samp(log(cap_xchgd_approx+100)) over (partition by Ticker order by Date rows between 5*52 preceding and current row) as std_cap
        , log(cap_xchgd_approx + 100) as log_cap
        , dayofweek(Date) as day_of_week
        , Date
        , Ticker
        , exit_gain_loss
        , exit_gain_loss as target
    from full_df
    where TMF_Simple_Signal = 1
    --and Ticker in string 
    """
    )
    train       = db.sql("select * from feature_target where year(Date) in (2005,2007,2009,2011,2013,2015)").df().dropna() # worked ????
    validate    = db.sql("select * from feature_target where year(Date) in (2017,2018,2019,2020,2021,2022)").df().dropna() # worked ????
    test        = db.sql("select * from feature_target where year(Date) in (2023,2024)").df().dropna()
    drop_columns = ['Date', 'Ticker', 'target', 'exit_gain_loss']
    # categories: metadata, eval_metric_related, target

    X_train, Y_train = train.drop(columns = drop_columns), train['target']
    X_validate, Y_validate = validate.drop(columns = drop_columns), validate['target']
    
    with mlflow.start_run(nested=True):
        # Log param
        # log early to detect failure
        mlflow.log_params(params)
        
        # model_train
        clf.fit(X_train,Y_train)

        # model eval
        Y_validate_predict = clf.predict(X_validate)

        # Log results
        res = {

        }
        for func in [
            mean_absolute_error,
            mean_squared_error,
            mean_poisson_deviance,
            mean_gamma_deviance,
            #mean_tweedie_deviance,
        ]:
            res[f"eval_{func.__name__}"] = func(Y_validate, Y_validate_predict)
            mlflow.log_metric(f"eval_{func.__name__}", func(Y_validate, Y_validate_predict))
        
        signature = infer_signature(X_train, Y_train) 

        models[trial.study.study_name][trial.number] = clf
        # Log model
        mlflow.sklearn.log_model(clf, "model", signature=signature)

    return res['eval_mean_poisson_deviance']#{"loss": eval_rmse, "status": STATUS_OK, "model": model}

# 3. Create a study object and optimize the objective function.
mlflow.set_experiment("/stock_signal_predict")
with mlflow.start_run():
    study = optuna.create_study(direction='minimize', sampler=TPESampler())
    study.optimize(objective, n_trials=200)
    
    mlflow.log_params(study.best_params)
    mlflow.log_metric("eval_mean_poisson_deviance", study.best_value)


    mlflow.sklearn.log_model(models[study.study_name][study.best_trial.number], "model")
    
    