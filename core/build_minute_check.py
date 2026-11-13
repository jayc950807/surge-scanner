#!/usr/bin/env python3
"""events 브랜치의 후보일 중 (일중폭>20% & 종가 위치<15%, 2020~) 에 대해 1분봉을 받아
① 15:30 시점(봇이 실제로 보는 데이터) 기준 값  ② 15:30 기준 조건을 통과한 것의 다음 3일 1분봉 저장."""
import os, sys, time
import numpy as np, pandas as pd
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(__file__))
import alpaca_data as ad
E = pd.read_parquet('/tmp/evsrc/events.parquet')
E['clpos'] = (E.c - E.l) / (E.h - E.l).clip(lower=1e-9)
E = E[(E.intra > 0.2) & (E.clpos < 0.15) & (E.date >= '2020-01-01')]
print('events', len(E), flush=True)
os.makedirs('/tmp/mc', exist_ok=True); snaps = []; mins = []; t0 = time.time()
for i, (d, g) in enumerate(E.groupby('date')):
    d0 = datetime.strptime(d, '%Y-%m-%d')
    syms = list(g.sym.unique())
    try:
        df = ad.minute_bars(syms, d0.replace(hour=9, minute=30), d0 + timedelta(days=7), adjustment='split', chunk=100)
    except Exception as e:
        print('err', d, e, flush=True); continue
    if df.empty: continue
    df['t'] = df['t'].dt.tz_convert(ad.NY).dt.tz_localize(None)
    df['hm'] = df.t.dt.hour * 60 + df.t.dt.minute; df = df[(df.hm >= 570) & (df.hm <= 960)]
    df['day'] = df.t.dt.normalize()
    for sym, s in df.groupby('symbol'):
        s = s.sort_values('t'); days = sorted(s.day.unique())
        if not days or days[0] != pd.Timestamp(d): continue
        z = s[s.day == days[0]]; a = z[z.hm <= 930]
        if a.empty: continue
        snaps.append(dict(sym=sym, date=d, o930=a.o.iloc[0], hi930=a.h.max(), lo930=a.l.min(), p930=a.c.iloc[-1],
                          v930=a.v.sum(), p945=z[z.hm <= 945].c.iloc[-1], close=z.c.iloc[-1], lo_day=z.l.min(), hi_day=z.h.max()))
        rng = (a.h.max() - a.l.min()) / max(a.l.min(), 1e-9); pos = (a.c.iloc[-1] - a.l.min()) / max(a.h.max() - a.l.min(), 1e-9)
        if rng > 0.2 and pos < 0.2:
            k = s[s.day.isin(days[:4])][['t', 'o', 'h', 'l', 'c', 'v']].copy(); k['sym'] = sym; k['sd'] = d
            mins.append(k)
    if i % 50 == 0: print(i, d, len(snaps), len(mins), f'{time.time()-t0:.0f}s', flush=True)
pd.DataFrame(snaps).to_parquet('/tmp/mc/snap.parquet', index=False)
M = pd.concat(mins, ignore_index=True); M['t'] = M.t.astype('datetime64[s]')
for c in 'ohlc': M[c] = M[c].astype('float32')
M.to_parquet('/tmp/mc/mins.parquet', index=False, compression='zstd')
print(f'::notice title=mc::snap {len(snaps)} · mins events {len(mins)} · rows {len(M):,}')
