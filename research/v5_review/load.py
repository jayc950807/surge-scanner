import pandas as pd, numpy as np, glob
P='/mnt/user-data/uploads/jayckim/surge-harness/data/'
def load():
    b=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(P+'derived/bars/*.parquet'))],ignore_index=True)
    b['date']=pd.to_datetime(b.date)
    b=b.sort_values(['ticker','date']).reset_index(drop=True)
    # 실제 가격 계수 (분할 보정 되돌리기)
    ca=pd.read_parquet(P+'extra_data/corporate_actions.parquet').drop_duplicates('id')
    ca=ca.replace({'None':np.nan,'nan':np.nan})
    ca['sym']=ca.symbol.fillna(ca.old_symbol)
    ca['ex']=pd.to_datetime(ca.ex_date.fillna(ca.process_date).fillna(ca.effective_date),errors='coerce')
    ca['old']=pd.to_numeric(ca.old_rate,errors='coerce'); ca['new']=pd.to_numeric(ca.new_rate,errors='coerce')
    spl=ca[ca.type.isin(['reverse_splits','forward_splits'])&ca.ex.notna()&(ca.old>0)&(ca.new>0)].copy()
    spl['mult']=spl.new/spl.old
    fac=np.ones(len(b))
    tk=b.ticker.values; starts=np.r_[0,np.flatnonzero(tk[1:]!=tk[:-1])+1]; ends=np.r_[starts[1:],len(b)]
    sidx=dict(zip(tk[starts],zip(starts,ends)))
    dn=b.date.values.astype('datetime64[ns]').astype(np.int64)
    for s,g in spl.sort_values('ex').groupby('sym'):
        if s not in sidx: continue
        a,e=sidx[s]; ts=g.ex.values.astype('datetime64[ns]').astype(np.int64)
        cum=np.r_[np.cumprod(g.mult.values[::-1])[::-1],1.0]
        k=np.searchsorted(ts,dn[a:e],side='right'); fac[a:e]=cum[k]
    b['raw_close']=b.close*fac
    return b
if __name__=='__main__':
    b=load(); b.to_parquet('bars_all.parquet'); print(len(b), b.ticker.nunique(), b.date.min(), b.date.max())
    print(b[b.ticker=='SMX'].tail(3))
