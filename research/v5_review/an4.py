import pandas as pd,numpy as np
b=pd.read_parquet('feat.parquet',columns=['ticker','date','open','high','low','close','raw_close','dvol20','intra','n','avgvol20'])
g=b.groupby('ticker',sort=False)
b['on']=g.open.shift(-1)/b.close-1
# Abdi-Ranaldo 스프레드 추정 (종가·고저 중간값 기반), 20일 평균
eta=(np.log(b.high)+np.log(b.low))/2; eta1=g['high'].shift(-1); 
eta_n=(np.log(g.high.shift(-1))+np.log(g.low.shift(-1)))/2
c=np.log(b.close)
s2=4*(c-eta)*(c-eta_n)
b['spr']=np.sqrt(s2.clip(lower=0)).groupby(b.ticker,sort=False).transform(lambda s:s.shift(1).rolling(20,min_periods=10).mean())
b['cost']=np.maximum(0.01,2*b.spr.fillna(0.02))
D={s:pd.read_parquet(f'D_{s}.parquet')[['ticker','date']] for s in ('S1','S3')}
base=(b.n>=25)&(b.avgvol20>=10000)&(b.raw_close>=1)
b['ret1']=g.close.pct_change()
R=b[base&(b.intra>0.2)]
R2=b[base&(b.intra>0.2)&(b.ret1<-0.08)]
def rep(name,x):
    for per,m in (('학습',x.date<'2023'),('검증',x.date>='2023')):
        y=x[m].dropna(subset=['on']); net=y.on-y.cost
        print(f"{name:4s} {per} n={len(y)} 비용전 평균 {y.on.mean()*100:+.2f} 중위 {y.on.median()*100:+.2f} | 추정비용 평균 {y.cost.mean()*100:.2f} | 비용후 평균 {net.mean()*100:+.2f} 중위 {net.median()*100:+.2f} 플러스비율 {(net>0).mean()*100:.0f}%")
for s,d in D.items():
    x=d.merge(b,on=['ticker','date']); rep(s,x)
    x['liq']=pd.cut(x.dvol20,[0,1e6,5e6,1e12],labels=['<100만$','100~500만$','>500만$'])
    t=x.groupby(['liq',x.date>='2023']).apply(lambda y:pd.Series({'n':len(y),'비용전':y.on.mean()*100,'비용후':(y.on-y.cost).mean()*100})).round(2)
    print(t)
rep('무작위(변동20%)',R)
rep('무작위(변동20%+당일-8%)',R2)
