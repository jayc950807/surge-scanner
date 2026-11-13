#!/usr/bin/env python3
"""2016~지금 전 종목(상장폐지 포함) 일봉 → '급락·과매도' 후보일(느슨한 조건)마다 특징 + 다음 7일 가격을 저장 (분석용).
v6 전략 1·3 은 이 후보들의 부분집합. 출력: /tmp/ev/events.parquet"""
import os, sys, time
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import alpaca_data as ad
from ohlcv_cache import EXCHANGES, NON_COMMON
from shared_config import LEVERAGED_ETF
from shared_config import calc_rsi_wilder

START = os.environ.get('EV_START', '2016-01-01'); NF = 7
a = pd.concat([ad.assets('active'), ad.assets('inactive')], ignore_index=True)
a['name'] = a['name'].fillna('').astype(str); a['symbol'] = a['symbol'].astype(str)
ok = (a['exchange'].isin(EXCHANGES) & a['symbol'].str.fullmatch(r'[A-Z]{1,5}') & ~a['symbol'].str.endswith('W')
      & ~a['symbol'].isin(LEVERAGED_ETF) & ~a['name'].str.contains(NON_COMMON))
syms = sorted(a.loc[ok, 'symbol'].unique()); print('symbols', len(syms), flush=True)
end = ad.now_ny().date()
os.makedirs('/tmp/ev', exist_ok=True); out = []; t0 = time.time()

def rsi(c, n):
    return calc_rsi_wilder(pd.Series(c), n)

for ci in range(0, len(syms), 100):
    ch = syms[ci:ci + 100]
    try:
        s = ad.daily_bars(ch, START, end, adjustment='split', chunk=100)
        r = ad.daily_bars(ch, START, end, adjustment='raw', chunk=100)[['symbol', 'date', 'c']].rename(columns={'c': 'raw_c'})
    except Exception as e:
        print('chunk err', ci, e, flush=True); continue
    if s.empty: continue
    s = s.merge(r, on=['symbol', 'date'], how='left')
    for sym, g in s.groupby('symbol'):
        g = g.sort_values('date')
        if len(g) < 30: continue
        o, h, l, c, v = (g[k].to_numpy(float) for k in ('o', 'h', 'l', 'c', 'v'))
        rc = g['raw_c'].to_numpy(float); dates = g['date'].to_numpy()
        n = len(c)
        r7 = rsi(c, 7).to_numpy(); r14 = rsi(c, 14).to_numpy()
        dn = np.zeros(n, int)
        for i in range(1, n): dn[i] = dn[i - 1] + 1 if c[i] < c[i - 1] else 0
        av20 = pd.Series(v).rolling(20).mean().to_numpy()
        dv20 = pd.Series(v * c).rolling(20).mean().to_numpy()
        low5 = pd.Series(l).rolling(5).min().to_numpy()
        hi20 = pd.Series(h).rolling(20).max().to_numpy(); hi60 = pd.Series(h).rolling(60, min_periods=20).max().to_numpy()
        intra = (h - l) / np.where(l > 0, l, np.nan)
        cand = np.where((r7 < 40) & (dn >= 2) & (intra > 0.10) & (rc >= 1.0) & (av20 >= 10000))[0]
        cand = cand[(cand >= 25) & (cand < n - 1)]
        for i in cand:
            row = dict(sym=sym, date=dates[i], o=o[i], h=h[i], l=l[i], c=c[i], raw_c=rc[i], v=v[i], av20=av20[i], dv20=dv20[i],
                       rsi7=r7[i], rsi14=r14[i], dn=dn[i], intra=intra[i], ret1=c[i] / c[i - 1] - 1, ret3=c[i] / c[i - 3] - 1,
                       ret5=c[i] / c[i - 5] - 1, ret20=c[i] / c[i - 20] - 1, dlow5=(c[i] - low5[i]) / max(low5[i], .01),
                       prev_dn=c[i - 1] < c[i - 2], gap=o[i] / c[i - 1] - 1, clpos=(c[i] - l[i]) / max(h[i] - l[i], 1e-9),
                       vrel=v[i] / max(av20[i], 1), dd20=c[i] / hi20[i] - 1, dd60=c[i] / hi60[i] - 1, nd=min(NF, n - 1 - i))
            for k in range(1, NF + 1):
                j = i + k
                if j < n:
                    row[f'o{k}'] = o[j]; row[f'h{k}'] = h[j]; row[f'l{k}'] = l[j]; row[f'c{k}'] = c[j]; row[f'd{k}'] = dates[j]
            out.append(row)
    print(ci, len(syms), len(out), f'{time.time() - t0:.0f}s', flush=True)

E = pd.DataFrame(out); E.to_parquet('/tmp/ev/events.parquet', index=False)
print(f'::notice title=events::종목 {len(syms)} · 후보일 {len(E):,}')
