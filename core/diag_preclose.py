#!/usr/bin/env python3
"""
점검용: 지난 날짜들에 대해 '15:30 ET 가격으로 1차_1·1차_3 조건에 맞았던 종목'을 전 종목에서 다시 찾아본다.
마감 전 스캔(감시목록만 확인)이 놓친 게 있는지, 감시목록이 걸러 버린 게 있는지 확인하는 용도. 주문과 무관.

  python core/diag_preclose.py 2026-10-19 2026-10-20 ...
결과: data/diag/preclose_check.csv (날짜·종목·15:30 신호·종가 신호·감시목록 포함 여부·지표) + 요약 출력(annotation)
"""
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import ohlcv_cache as cache
import alpaca_data as ad
import scanner
from shared_config import calc_rsi_wilder, WATCHLIST_RSI7, WATCHLIST_CONSEC, MIN_PRICE, MIN_VOLUME


def wl_rule(df):
    """감시목록 규칙(전날 데이터): RSI7<45, 연속하락≥3, 실제가≥0.9, 20일 평균거래량≥1만"""
    if len(df) < 12:
        return False
    c = df['Close'].astype(float)
    r = calc_rsi_wilder(c, 7).iloc[-1]
    return (not pd.isna(r) and r < WATCHLIST_RSI7 and scanner.calc_consec_down(c) >= WATCHLIST_CONSEC
            and float(df['raw_close'].iloc[-1]) >= MIN_PRICE * 0.9 and float(df['Volume'].tail(20).mean()) >= MIN_VOLUME)


def main(dates):
    uni = set(cache.load_universe()['symbol'])
    rows = []
    for D in dates:
        full = cache.load(days=140, asof=D, symbols=uni)
        prev = full[full['date'] < D]; today = full[full['date'] == D]
        fr_prev = cache.per_ticker(prev)
        fr_full = cache.per_ticker(full[full['date'] <= D])
        # 넓게 후보: 전날 RSI7 < 60 (감시목록보다 훨씬 넓게)
        cand = []
        for tk, f in fr_prev.items():
            if len(f) < 24:
                continue
            r = calc_rsi_wilder(f['Close'].astype(float), 7).iloc[-1]
            if not pd.isna(r) and r < 60:
                cand.append(tk)
        start = datetime.combine(pd.to_datetime(D).date(), datetime.min.time().replace(hour=9, minute=30), ad.NY)
        end = datetime.combine(pd.to_datetime(D).date(), datetime.min.time().replace(hour=15, minute=30), ad.NY)
        mb = ad.minute_bars(cand, start, end, adjustment='raw', chunk=100)
        mins = {}
        if len(mb):
            ts = mb['t'].dt.tz_convert(ad.NY); hm = ts.dt.hour * 60 + ts.dt.minute
            mb = mb[(hm >= 570) & (hm <= 930)]
            for tk, g in mb.groupby('symbol'):
                mins[tk] = g.sort_values('t')
        n_bar = 0
        for tk in cand:
            f = fr_prev[tk]
            in_wl = wl_rule(f)
            # 15:30 임시봉
            s1530, px = [], np.nan
            g = mins.get(tk)
            if g is not None and len(g):
                n_bar += 1
                bar = dict(Open=float(g['o'].iloc[0]), High=float(g['h'].max()), Low=float(g['l'].min()), Close=float(g['c'].iloc[-1]), Volume=float(g['v'].sum()))
                px = bar['Close']
                df2 = pd.concat([f[['Open', 'High', 'Low', 'Close', 'Volume']], pd.DataFrame([bar], index=[pd.Timestamp(D)])])
                s1530 = [s for s, _ in scanner.check_signals(df2)]
            # 종가 기준
            sclose = []
            ff = fr_full.get(tk)
            if ff is not None and len(ff) and ff.index[-1].strftime('%Y-%m-%d') == D:
                sclose = [s for s, _ in scanner.check_signals(ff[['Open', 'High', 'Low', 'Close', 'Volume']])]
            if s1530 or sclose:
                rows.append({'date': D, 'ticker': tk, 'sig_1530': '+'.join(s1530), 'sig_close': '+'.join(sclose), 'in_watchlist': in_wl,
                             'px_1530': round(px, 4) if not np.isnan(px) else '', 'close': float(ff['Close'].iloc[-1]) if ff is not None else ''})
        hit = [r for r in rows if r['date'] == D]
        a = [r['ticker'] for r in hit if r['sig_1530']]
        b = [r['ticker'] for r in hit if r['sig_close']]
        miss_wl = [r['ticker'] for r in hit if r['sig_1530'] and not r['in_watchlist']]
        msg = f'{D}: 후보 {len(cand)} · 1분봉 {n_bar} · 15:30 조건 {len(a)} {a} · 종가 조건 {len(b)} {b} · 감시목록 밖 15:30 신호 {miss_wl}'
        print(msg); print(f'::notice title=diag::{msg}'[:1000])
    os.makedirs('data/diag', exist_ok=True)
    pd.DataFrame(rows, columns=['date', 'ticker', 'sig_1530', 'sig_close', 'in_watchlist', 'px_1530', 'close']).to_csv('data/diag/preclose_check.csv', index=False)


if __name__ == '__main__':
    main(sys.argv[1:])
