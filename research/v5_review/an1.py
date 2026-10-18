import pandas as pd, numpy as np, itertools
from grid_eval import evalp
COST=0.01
D={s:(pd.read_parquet(f'D_{s}.parquet'),np.load(f'P_{s}.npy')) for s in ('S1','S3','RND')}
TR=lambda d:(d.date<'2023-01-01').values; TE=lambda d:(d.date>='2023-01-01').values
rows=[]
for tp,sl,h in itertools.product([0.03,0.05,0.07,0.10,0.15,None],[-0.1,-0.15,-0.2,-0.3,None],[1,2,3,5,10]):
    for s,(d,P) in D.items():
        r=evalp(P,tp,sl,h)-COST
        for per,m in (('train',TR(d)),('test',TE(d))):
            x=r[m]; x=x[~np.isnan(x)]
            rows.append(dict(s=s,tp=tp,sl=sl,h=h,per=per,n=len(x),mean=x.mean()*100,med=np.median(x)*100,hit=(x>0).mean()*100))
R=pd.DataFrame(rows)
W=R.pivot_table(index=['s','tp','sl','h'],columns='per',values=['mean','n','hit'],dropna=False)
W.columns=[f'{a}_{b}' for a,b in W.columns]; W=W.reset_index()
rnd=W[W.s=='RND'].set_index(['tp','sl','h'])[['mean_train','mean_test']].add_prefix('rnd_')
W=W.join(W[['tp','sl','h']].apply(tuple,axis=1).map(lambda k: rnd.loc[k] if k in rnd.index else None).apply(pd.Series),how='left') if False else W.merge(rnd.reset_index(),on=['tp','sl','h'],how='left')
W['edge_train']=W.mean_train-W.rnd_mean_train; W['edge_test']=W.mean_test-W.rnd_mean_test
W.to_csv('exit_grid.csv',index=False)
pd.set_option('display.width',250)
for s in ('S1','S3'):
    w=W[W.s==s]
    print('==',s,'현재설정(5%,-20%,5일):'); print(w[(w.tp==0.05)&(w.sl==-0.2)&(w.h==5)].round(2).to_string(index=False))
    print(' 학습기간 최고 10개 → 검증기간'); print(w.sort_values('mean_train',ascending=False).head(10).round(2).to_string(index=False))
    print(' 학습-검증 상관', round(w[['mean_train','mean_test']].corr().iloc[0,1],2))
print('== RND 현재설정'); w=W[W.s=='RND']; print(w[(w.tp==0.05)&(w.sl==-0.2)&(w.h==5)].round(2).to_string(index=False))
