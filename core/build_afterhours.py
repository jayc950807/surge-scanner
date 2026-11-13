#!/usr/bin/env python3
"""장 마감 후(16:00~20:00) 매수 가능성 확인: 종가 최저 근처 후보(일중폭>20% & 종가 위치<5%, 2020~)의
신호날 장후 + 다음 2일(장전·정규·장후) 1분봉 저장."""
import os, sys, time
import numpy as np, pandas as pd
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(__file__))
import alpaca_data as ad
E = pd.read_parquet('/tmp/evsrc/events.parquet')
E['clpos'] = (E.c - E.l) / (E.h - E.l).clip(lower=1e-9)
E = E[(E.intra > 0.2) & (E.clpos < 0.05) & (E.date >= '2020-01-01')]
print('events', len(E), flush=True)
os.makedirs('/tmp/ah', exist_ok=True); parts = []; t0 = time.time()
for i, (d, g) in enumerate(E.groupby('date')):
    d0 = datetime.strptime(d, '%Y-%m-%d')
    try:
        df = ad.minute_bars(list(g.sym.unique()), d0.replace(hour=15, minute=0), d0 + timedelta(days=5), adjustment='split', chunk=100)
    except Exception as e:
        print('err', d, e, flush=True); continue
    if df.empty: continue
    df['t'] = df['t'].dt.tz_convert(ad.NY).dt.tz_localize(None)
    df = df[['symbol', 't', 'o', 'h', 'l', 'c', 'v']].rename(columns={'symbol': 'sym'}); df['sd'] = d
    parts.append(df)
    if i % 50 == 0: print(i, d, f'{time.time()-t0:.0f}s', flush=True)
M = pd.concat(parts, ignore_index=True); M['t'] = M.t.astype('datetime64[s]')
for c in 'ohlcv': M[c] = M[c].astype('float32')
M.to_parquet('/tmp/ah/ah.parquet', index=False, compression='zstd')
print(f'::notice title=ah::events {len(E)} rows {len(M):,}')
