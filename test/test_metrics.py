import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import adv_predict
from adv_predict.metrics import monthly_win_ratio_avg, monthly_win_ratio_std, mthly_rtrn_pct_std, mthly_rtrn_pct_avg

import numpy as np 
d = np.array(['2001-01-01', '2001-01-02', '2001-02-01', '2001-02-02', '2001-03-01', '2001-03-02', '2001-04-01', '2001-04-02'], dtype='datetime64')
t = np.array([0.95,1.02 ,1.03,0.95 ,0.95,0.8 ,1.1,1.1])
p = np.array([1.02,1.03 ,1.04,1.05 ,1.1,1.1  ,0.95,0.8])

print(
    'avg', monthly_win_ratio_avg(t,p,1,d)
)

print(
    'std', monthly_win_ratio_std(t,p,1,d)
)

print(
    'std', mthly_rtrn_pct_std(t,p,1,d)
)

print(
    'std', mthly_rtrn_pct_avg(t,p,1,d)
)
