import pandas as pd, numpy as np, re, itertools
b=pd.read_parquet('feat.parquet')
a=pd.read_parquet('/mnt/user-data/uploads/jayckim/surge-harness/data/extra_data/assets.parquet')
nm=dict(zip(a.symbol,a.name.fillna('')))
LEV=re.compile(r'(\b[23]X\b|2x|3x|Leveraged|Ultra|Inverse|-1X|\bBull\b|\bBear\b)',re.I)
ETF=re.compile(r'\b(ETF|ETN|Fund)\b',re.I)
b['etf']=b.ticker.map(lambda t: bool(ETF.search(nm.get(t,'')) or LEV.search(nm.get(t,''))))
# 시장 분위기: 그날 전 종목 중위 수익률, 직전 5일 누적
mk=b.groupby('date').ret1.median(); mk5=(1+mk).rolling(5).apply(np.prod,raw=True)-1
b['mkt1']=b.date.map(mk); b['mkt5']=b.date.map(mk5)
base=(b.n>=25)&(b.avgvol20>=10000)&(b.raw_close>=1)
s1=(b.rsi7<20)&(b.intra>0.20)&(b.consec>5)&(b.dist5<0.05)&(b.ret3<-0.15)
s3=(b.rsi7<30)&(b.intra>0.20)&(b.consec>3)&(b.dist5<0.03)&(b.ret1<-0.08)&b.prev_down
b['S1']=base&s1; b['S3']=base&s3
# 무작위 기준선 후보: 같은 날, 기본 필터 + 일중변동 20% 초과(변동 큰 종목)
b['RND']=base&(b.intra>0.20)
T=b.ticker.values;O=b.open.values;H=b.high.values;L=b.low.values;C=b.close.values;N=len(b)
K=10
def paths(idx):
    P=np.full((len(idx),K,4),np.nan); ok=np.zeros(len(idx),bool)
    for r,i in enumerate(idx):
        j0=i+1
        if j0>=N or T[j0]!=T[i]: continue
        ok[r]=True
        for k in range(K):
            j=j0+k
            if j>=N or T[j]!=T[i]:
                P[r,k:,:]=P[r,k-1,3] if k>0 else np.nan; break  # 끝나면 마지막 종가로 고정
            P[r,k]=(O[j],H[j],L[j],C[j])
    return P,ok
def evalp(P,tp,sl,hold):
    ep=P[:,0,0]; n=len(ep); r=np.full(n,np.nan); done=np.zeros(n,bool)
    tpp=ep*(1+tp) if tp else np.full(n,np.inf); slp=ep*(1+sl) if sl else np.full(n,-np.inf)
    for k in range(hold):
        o,h,l,c=P[:,k,0],P[:,k,1],P[:,k,2],P[:,k,3]
        if k>0:
            g=~done&(o<=slp); r[g]=o[g]/ep[g]-1; done|=g
            g=~done&(o>=tpp); r[g]=o[g]/ep[g]-1; done|=g
        th=h>=tpp; sh=l<=slp
        g=~done&th&sh; r[g]=sl; done|=g
        g=~done&th; r[g]=tp; done|=g
        g=~done&sh; r[g]=sl; done|=g
        if k==hold-1:
            g=~done; r[g]=c[g]/ep[g]-1; done|=g
    return r
cols=['ticker','date','raw_close','dvol20','etf','rsi7','intra','consec','ret1','ret3','dist5','mkt1','mkt5']
out={}
for s in ('S1','S3'):
    idx=np.flatnonzero(b[s].values); P,ok=paths(idx)
    d=b.iloc[idx][cols].reset_index(drop=True); d=d[ok].reset_index(drop=True); P=P[ok]
    np.save(f'P_{s}.npy',P); d.to_parquet(f'D_{s}.parquet'); out[s]=(d,P)
# 무작위 기준선 표본 (최대 20만)
ridx=np.flatnonzero(b['RND'].values); rng=np.random.default_rng(0); ridx=np.sort(rng.choice(ridx,min(200000,len(ridx)),replace=False))
P,ok=paths(ridx); d=b.iloc[ridx][cols].reset_index(drop=True)[ok].reset_index(drop=True); np.save('P_RND.npy',P[ok]); d.to_parquet('D_RND.parquet')
print({k:len(v[0]) for k,v in out.items()}, len(d))
