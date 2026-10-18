import pandas as pd, numpy as np
from grid_eval import evalp
COST=0.01
L=[]
for s in ('S1','S3'):
    d=pd.read_parquet(f'D_{s}.parquet'); P=np.load(f'P_{s}.npy')
    d['r']=evalp(P,0.05,-0.2,5)-COST; d['s']=s; L.append(d)
d=pd.concat(L).drop_duplicates(['ticker','date']).dropna(subset=['r'])
d['dow']=d.date.dt.dayofweek
d['tr']=d.date<'2023'
for f in ['raw_close','dvol20','rsi7','intra','ret1','ret3','consec','dist5','mkt1','mkt5']:
    q=pd.qcut(d[f].rank(method='first'),5,labels=False)
    t=d.groupby([q,'tr']).r.agg(['mean','count']).unstack()
    rng=d.groupby(q)[f].agg(['min','max'])
    print(f'— {f}'); 
    for k in range(5): print(f"   Q{k+1} [{rng.loc[k,'min']:.3g}~{rng.loc[k,'max']:.3g}] 학습 {t.loc[k,('mean',True)]*100:+.2f}({t.loc[k,('count',True)]:.0f})  검증 {t.loc[k,('mean',False)]*100:+.2f}({t.loc[k,('count',False)]:.0f})")
print('— etf', d.groupby(['etf','tr']).r.agg(['mean','count']))
print('— dow', d.groupby(['dow','tr']).r.mean().unstack().round(4)*100)
