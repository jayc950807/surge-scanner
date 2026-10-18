import pandas as pd, numpy as np
from grid_eval import evalp
COST=0.01
def rob(s,tp,sl,h,mask=None,label=''):
    d=pd.read_parquet(f'D_{s}.parquet'); P=np.load(f'P_{s}.npy')
    if mask is not None: m=mask(d).values; d=d[m].reset_index(drop=True); P=P[m]
    r=evalp(P,tp,sl,h)-COST; d['r']=r; d=d.dropna(subset=['r'])
    out=[]
    for per,dd in (('train',d[d.date<'2023']),('test',d[d.date>='2023'])):
        x=np.sort(dd.r.values); n=len(x)
        if n==0: continue
        t5=x[:int(n*0.95)].mean() if n>20 else np.nan
        # 날짜 묶음 부트스트랩
        g=dd.groupby('date').r.mean(); rng=np.random.default_rng(1)
        bs=[rng.choice(g.values,len(g)).mean() for _ in range(1000)]
        out.append(f"{per}: n={n} 평균={x.mean()*100:+.2f} 중위={np.median(x)*100:+.2f} 상위5%제외={t5*100:+.2f} 하루평균의 95%CI=[{np.percentile(bs,2.5)*100:+.2f},{np.percentile(bs,97.5)*100:+.2f}]")
    yr=d.groupby(d.date.dt.year).r.agg(['count','mean']); yr['mean']*=100
    print(f"■ {s} {label} TP={tp} SL={sl} H={h}"); [print('   ',o) for o in out]
    print('    연도별:',' '.join(f"{y}:{m:+.1f}({c})" for y,(c,m) in yr.iterrows()))
for s in ('S1','S3'):
    rob(s,0.05,-0.2,5,label='현재')
    rob(s,None,-0.2,10); rob(s,0.10,None,10); rob(s,0.07,None,10); rob(s,0.15,-0.2,10)
d=pd.read_parquet('D_RND.parquet')
rob('RND',0.05,-0.2,5,label='무작위'); rob('RND',None,-0.2,10,label='무작위'); rob('RND',0.10,None,10,label='무작위')
