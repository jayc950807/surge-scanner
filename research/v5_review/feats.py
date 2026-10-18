import pandas as pd, numpy as np
b=pd.read_parquet('bars_all.parquet')
g=b.groupby('ticker',sort=False)
C,H,L,O,V=b.close,b.high,b.low,b.open,b.volume.astype(float)
first=b.ticker.ne(b.ticker.shift())
pc=C.shift(); pc[first]=np.nan
d=C.diff(); d[first]=np.nan
up=d.clip(lower=0); dn=(-d).clip(lower=0)
# Wilder RSI7 (ewm alpha=1/7) per ticker
ag=up.groupby(b.ticker,sort=False).transform(lambda s: s.ewm(alpha=1/7,adjust=False,min_periods=7).mean())
al=dn.groupby(b.ticker,sort=False).transform(lambda s: s.ewm(alpha=1/7,adjust=False,min_periods=7).mean())
b['rsi7']=100-100/(1+ag/al.replace(0,1e-10))
b['intra']=(H-L)/L
down=(C<pc).astype(int)
# consecutive down count
grp=(down==0).groupby(b.ticker,sort=False).cumsum()
b['consec']=down.groupby([b.ticker,grp],sort=False).cumsum()
low5=L.groupby(b.ticker,sort=False).transform(lambda s:s.rolling(5).min())
b['dist5']=(C-low5)/low5.clip(lower=0.01)
for k in (1,3):
    p=C.groupby(b.ticker,sort=False).shift(k); b[f'ret{k}']=C/p-1
b['prev_down']=(pc< C.groupby(b.ticker,sort=False).shift(2))
b['avgvol20']=V.groupby(b.ticker,sort=False).transform(lambda s:s.rolling(20,min_periods=10).mean())
b['dvol20']=(C*V).groupby(b.ticker,sort=False).transform(lambda s:s.rolling(20,min_periods=10).mean())
b['n']=g.cumcount()
b.to_parquet('feat.parquet')
print('done',len(b))
