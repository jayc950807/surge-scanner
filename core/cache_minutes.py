#!/usr/bin/env python3
"""과거 신호(v6 1·3, 마감 전 탐지)마다 신호 날부터 약 3주 1분봉(장 전·후 포함)을 받아 한 파일로 저장 → 분석용(artifact).
출력: /tmp/cache/minutes.parquet (sym, sd, t[뉴욕 시각], o,h,l,c,v), /tmp/cache/signals.csv"""
import os, sys
import pandas as pd
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(__file__))
import alpaca_data as ad
from sim_realistic import trades

os.makedirs('/tmp/cache', exist_ok=True)
x = trades()
x.to_csv('/tmp/cache/signals.csv', index=False)
parts = []; miss = 0
for i, r in enumerate(x.itertuples()):
    d0 = datetime.strptime(r.signal_date, '%Y-%m-%d')
    try:
        m = ad.history_minute(r.ticker, d0.replace(hour=4), d0 + timedelta(days=22), adjustment='raw', regular_only=False)
    except Exception as e:
        m = None; print('err', r.ticker, r.signal_date, e)
    if m is None or not len(m):
        miss += 1; continue
    m = m.reset_index().rename(columns={'index': 't'})
    m.columns = ['t'] + [c.lower()[0] if c.lower() != 'volume' else 'v' for c in m.columns[1:]]
    m['sym'] = r.ticker; m['sd'] = r.signal_date
    parts.append(m)
    if i % 20 == 0: print(i, len(x))
out = pd.concat(parts, ignore_index=True)
out.to_parquet('/tmp/cache/minutes.parquet', index=False)
print(f'::notice title=cache::신호 {len(x)} · 받음 {len(parts)} · 없음 {miss} · 행 {len(out):,}')
