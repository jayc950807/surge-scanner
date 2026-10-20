#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Strategy 1 + 3 (v6, 데이터: Alpaca + 일봉 캐시)

  Strategy 1 (급락반등):
    신호: Intra>20% + Ret3d<-15% + ConsecDown>5 + DistLow5<5% + RSI7<20
  Strategy 3 (과매도):
    신호: RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + ConsecDown>3 + DistLow5<3%

  공통 매매 규칙 (v6, research/STRATEGY_V6.md):
    탐지: 마감 전 스캔 — 감시목록(전날 RSI7<45 & 연속하락≥3)만 오늘 1분봉으로 확인.
          무료 플랜은 15분 지연이라 15:46 ET 에 돌려 15:30 까지의 가격으로 판정한다.
    진입: 신호일 마감가 (종가 주문, MOC)
    청산: +5% 익절 | -10% 손절 (갭으로 뚫리면 시가 체결) | 2거래일 타임아웃 (마지막 날 종가)
    비용: 왕복 COST_PCT (shared_config) 를 결과에서 차감
    제외: ETF·ETN·레버리지 상품, 워런트·유닛 등 보통주 아닌 것, 실제 주가 $1 미만, 20일 평균 거래량 1만주 미만

  데이터 (core/alpaca_data.py, core/ohlcv_cache.py):
    - 일봉은 data/ohlcv/ 에 하루 한 파일로 보관. 마감 뒤 스캔은 '빠진 날'만 받아 붙인다(평소 하루치, 10초쯤).
    - 마감 뒤 전 종목 스캔은 캐시만으로 판정(네트워크 없음) + 내일 감시목록 저장. 기록·대조용이라
      거기서만 잡힌 신호는 추적하지 않는다(scan_type='afterclose').
    - 마감 전 스캔은 감시목록 종목의 오늘 1분봉(09:30~지금-16분)을 받아 임시 일봉으로 합쳐 판정.

  Usage:
    python scanner.py                   # 마감 뒤: 캐시 갱신(마지막 확정 거래일까지) + 전 종목 스캔. 언제 돌려도 안전, 하루 한 번만 실제 실행
    python scanner.py --preclose        # 15:40~15:48 ET 감시목록 빠른 확인 → 마감가 매수용 신호
    python scanner.py --build-watchlist # 캐시로 감시목록만 다시 생성 (네트워크 없음)
    python scanner.py --rebuild-cache   # 캐시를 지우고 Alpaca 에서 처음부터 다시 받음
    python scanner.py --strategy 1      # 전략 1만

  Output: data/signal_YYYY-MM-DD.csv + data/history.csv (append) + data/latest_scan.json + data/watchlist.csv
================================================================================
"""

import pandas as pd
import numpy as np
import time
import warnings
import os
import glob
import json
import argparse
from datetime import datetime, timezone, timedelta

from shared_config import (
    KST,
    MIN_PRICE,
    MIN_VOLUME,
    STRATEGY_CONFIG,
    STRATEGY_NAMES,
    ACTIVE_STRATEGIES,
    WATCHLIST_RSI7,
    WATCHLIST_CONSEC,
    calc_rsi_wilder,
    get_expected_trading_date,
    get_last_trading_date,
    is_us_trading_day,
    is_us_dst,
)
import ohlcv_cache as cache
import alpaca_data

warnings.filterwarnings('ignore')

# ─── Strategy thresholds ─────────────────────────────────────────────────────
# Strategy 1 (급락반등)
A_INTRA_THRESH   = 0.20     # 일중 변동폭 > 20%
A_RET3D_THRESH   = -0.15    # 3일 수익률 < -15%
A_CONSEC_DOWN    = 5        # 연속 하락 > 5일
A_DIST_LOW5      = 0.05     # 5일 최저가 대비 < 5%
A_RSI7_THRESH    = 20       # RSI(7) < 20

# Strategy 3 (과매도)
C_RSI7_THRESH    = 30       # RSI(7) < 30
C_INTRA_THRESH   = 0.20     # 일중 변동폭 > 20%
C_RET1D_THRESH   = -0.08    # 당일 수익률 < -8%
C_CONSEC_DOWN    = 3        # 연속 하락 > 3일
C_DIST_LOW5      = 0.03     # 5일 최저가 대비 < 3%

PHASE1_RSI7      = 35       # 1차 거르기 (두 전략 공통, 느슨하게)
WATCHLIST_PATH   = 'data/watchlist.csv'
HISTORY_DAYS     = 130      # 판정에 쓰는 일봉 길이 (캐시에서 불러오는 거래일 수)

PRECLOSE_WINDOW_ET = (15 * 60 + 40, 15 * 60 + 48)   # 마감 전 스캔 허용 시각 (분): 15:40 ~ 15:48 ET (MOC 주문 마감 15:50 전)
PRECLOSE_DATA_LAG_MIN = alpaca_data.SIP_DELAY_MIN    # 무료 플랜 지연 → 15:46 실행 = 15:30 데이터


# ─── Helper Functions ─────────────────────────────────────────────────────────

def calc_consec_down(close_series):
    """연속 하락일 수 (현재 시점 기준 역방향 카운트)"""
    n = len(close_series)
    if n < 2:
        return 0
    count = 0
    for i in range(n-1, 0, -1):
        if close_series.iloc[i] < close_series.iloc[i-1]:
            count += 1
        else:
            break
    return count


def _ref_prices(price):
    """참고용 목표가/손절가 (15:30 가격 또는 종가 기준). 실제 추적은 확정 종가 기준으로 다시 계산된다."""
    return round(price * (1 + STRATEGY_CONFIG['1']['tp_pct']), 2), round(price * (1 + STRATEGY_CONFIG['1']['sl_pct']), 2)


def _now_et():
    return datetime.now(timezone(timedelta(hours=-4 if is_us_dst(datetime.now(timezone.utc).date()) else -5)))


def _today_et():
    return _now_et().strftime('%Y-%m-%d')


CLOSE_SETTLED_ET = (16, 20)     # 이 시각(ET) 이후라야 오늘 일봉을 '확정'으로 본다 (종가 체결 + 무료 플랜 15분 지연 여유)


def _last_session_et():
    """마지막으로 '확정된' 미국 정규장 날짜(ET). 16:20 ET 이후면 오늘, 아니면 직전 거래일.
    (16:00~16:20 사이는 종가가 아직 안 들어왔을 수 있어 어제로 본다)"""
    now_et = _now_et()
    d = now_et.date()
    if (now_et.hour, now_et.minute) >= CLOSE_SETTLED_ET and is_us_trading_day(d):
        return d.strftime('%Y-%m-%d')
    return get_last_trading_date(d).strftime('%Y-%m-%d')


def already_scanned(through):
    """오늘(through) 마감 뒤 스캔을 이미 끝냈는지 — 캐시가 그날까지 있고 latest_scan.json 이 그날 Alpaca 스캔이면 참.
    GitHub 예약이 늦어질 때를 대비해 여러 번 걸어 두므로, 두 번째부터는 여기서 바로 끝난다."""
    try:
        if not cache.last_cached_day() or cache.last_cached_day() < through:
            return False
        with open('data/latest_scan.json') as f:
            j = json.load(f)
        return j.get('scan_date') == through and j.get('data_source') == 'alpaca' and j.get('mode') == 'afterclose'
    except Exception:
        return False


# ─── 신호 판정 ────────────────────────────────────────────────────────────────

def check_signals(df, run_1=True, run_3=True):
    """한 종목의 일봉(DataFrame: Open/High/Low/Close/Volume)으로 전략 1·3 조건을 판정.
    반환: [(strategy, fields_dict), ...]  — 테스트에서 직접 호출할 수 있게 분리."""
    out = []
    if df is None or len(df) < 25:
        return out
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    opn = df['Open'].astype(float)
    vol = df['Volume'].astype(float)
    n = len(close)

    c_last = float(close.iloc[-1]); h_last = float(high.iloc[-1])
    l_last = float(low.iloc[-1]); o_last = float(opn.iloc[-1])
    if o_last <= 0 or l_last <= 0 or c_last < MIN_PRICE:
        return out
    if float(vol.tail(20).mean()) < MIN_VOLUME:
        return out

    rsi7 = calc_rsi_wilder(close, 7)
    if pd.isna(rsi7.iloc[-1]):
        return out
    rsi7_val = float(rsi7.iloc[-1])

    intra = (h_last - l_last) / l_last
    consec = calc_consec_down(close)
    low5_min = float(low.iloc[-5:].min())
    dist_low5 = (c_last - low5_min) / max(low5_min, 0.01)
    prev_close = float(close.iloc[-2]); prev2_close = float(close.iloc[-3])
    ret1d = (c_last - prev_close) / max(prev_close, 0.01)
    ret3d = c_last / float(close.iloc[-4]) - 1

    common = {
        'price': round(c_last, 2), 'rsi7': round(rsi7_val, 1), 'intraday': round(intra * 100, 1),
        'consec_down': consec, 'dist_low5': round(dist_low5 * 100, 2),
        'ret1d': round(ret1d * 100, 1), 'ret3d': round(ret3d * 100, 1),
        'avg_vol20': int(vol.tail(20).mean()),
    }
    tp_ref, sl_ref = _ref_prices(c_last)
    common.update({'tp_price': tp_ref, 'sl_price': sl_ref})

    if (run_1 and rsi7_val < A_RSI7_THRESH and intra > A_INTRA_THRESH and consec > A_CONSEC_DOWN
            and dist_low5 < A_DIST_LOW5 and ret3d < A_RET3D_THRESH):
        out.append(('1', dict(common)))
    if (run_3 and rsi7_val < C_RSI7_THRESH and intra > C_INTRA_THRESH and consec > C_CONSEC_DOWN
            and dist_low5 < C_DIST_LOW5 and ret1d < C_RET1D_THRESH and prev_close < prev2_close):
        out.append(('3', dict(common)))
    return out


def scan_frames(frames, strat_str, asof, scan_type, write_watchlist=True, scan_time_kst=None):
    """{ticker: 일봉 DataFrame} 전체를 판정. 감시목록(내일 마감 전 스캔용)도 같은 데이터로 만든다.
    반환: (signals_1, signals_3, watchlist DataFrame)"""
    scan_time_kst = scan_time_kst or datetime.now(KST).strftime('%H:%M KST')
    run_1, run_3 = '1' in strat_str, '3' in strat_str
    signals = {'1': [], '3': []}
    wl_rows = []
    n_cand = 0
    for tk, df in frames.items():
        try:
            if df is None or len(df) < 12:
                continue
            close = df['Close'].astype(float)
            last_close = float(close.iloc[-1])
            raw_last = float(df['raw_close'].iloc[-1]) if 'raw_close' in df.columns else last_close
            avg_vol = float(df['Volume'].astype(float).tail(20).mean())
            if raw_last < MIN_PRICE * 0.9 or avg_vol < MIN_VOLUME:
                continue
            rsi7 = calc_rsi_wilder(close, 7)
            if pd.isna(rsi7.iloc[-1]):
                continue
            r7 = float(rsi7.iloc[-1])
            if r7 >= max(PHASE1_RSI7, WATCHLIST_RSI7):
                continue
            cd = calc_consec_down(close)
            if write_watchlist and r7 < WATCHLIST_RSI7 and cd >= WATCHLIST_CONSEC:
                wl_rows.append({'ticker': tk, 'asof': df.index[-1].strftime('%Y-%m-%d'), 'close': round(raw_last, 4),
                                'rsi7': round(r7, 1), 'consec_down': cd, 'avg_vol20': int(avg_vol)})
            if r7 >= PHASE1_RSI7 or len(df) < 25:
                continue
            n_cand += 1
            for strat, fields in check_signals(df, run_1, run_3):
                rec = {'strategy': strat, 'ticker': tk, 'date': asof, 'scan_time': scan_time_kst, 'scan_type': scan_type}
                rec.update(fields)
                signals[strat].append(rec)
                print(f"    ★ [{strat}] {tk} @ ${fields['price']:.2f} | RSI7={fields['rsi7']:.1f} "
                      f"Intra={fields['intraday']:.0f}% Ret1d={fields['ret1d']:.1f}% "
                      f"Ret3d={fields['ret3d']:.1f}% Down={fields['consec_down']}d Dist5={fields['dist_low5']:.1f}%")
        except Exception as e:
            print(f"  Warning: {tk} processing failed: {e}")
    wl = pd.DataFrame(wl_rows, columns=['ticker', 'asof', 'close', 'rsi7', 'consec_down', 'avg_vol20']).sort_values('ticker') if wl_rows else \
        pd.DataFrame(columns=['ticker', 'asof', 'close', 'rsi7', 'consec_down', 'avg_vol20'])
    print(f"  판정: {len(frames)}종목 중 RSI7<{PHASE1_RSI7} 후보 {n_cand}종목 | 감시목록 {len(wl)}종목")
    return signals['1'], signals['3'], wl


def save_watchlist(wl):
    os.makedirs('data', exist_ok=True)
    wl.to_csv(WATCHLIST_PATH, index=False)
    print(f"  감시목록 저장: {WATCHLIST_PATH} ({len(wl)}종목, 기준일 {wl['asof'].max() if len(wl) else '-'})")


def afterclose_scan(strat_str, write_watchlist=True):
    """캐시(마감 뒤)로 전 종목 판정 + 감시목록. 네트워크를 쓰지 않는다."""
    uni = cache.load_universe()
    symbols = set(uni['symbol']) if len(uni) else None          # 운영 대상(보통주)만. 목록이 없으면 캐시 전체
    df = cache.load(days=HISTORY_DAYS, symbols=symbols)
    if df.empty:
        print("  캐시가 비어 있음 (data/ohlcv/)"); return [], [], None
    asof = str(df['date'].max())
    print(f"\n[마감 뒤 스캔] 캐시 {df['ticker'].nunique()}종목 · {df['date'].min()} ~ {asof}")
    frames = cache.per_ticker(df)
    s1, s3, wl = scan_frames(frames, strat_str, asof, 'afterclose', write_watchlist)
    if write_watchlist:
        save_watchlist(wl)
    return s1, s3, asof


# ─── 마감 전 스캔 ─────────────────────────────────────────────────────────────

def market_still_open(now_et):
    """Alpaca clock 으로 '지금 장이 열려 있는지' 확인 (추수감사절 다음 날 같은 13:00 조기 마감 대비).
    확인 자체가 실패하면 None(판단 불가 → 진행)."""
    try:
        c = alpaca_data.clock()
        return bool(c.get('is_open')) if c else None
    except Exception as e:
        print(f"  ⚠ 장 운영 확인 실패: {e}")
        return None


def minute_bars_today(tickers, today):
    """오늘 09:30 ET ~ (지금-16분) 1분봉 → {ticker: DataFrame(Open/High/Low/Close/Volume, 뉴욕 시각 인덱스)} (정규장만)."""
    start = datetime.combine(pd.to_datetime(today).date(), datetime.min.time().replace(hour=9, minute=30), alpaca_data.NY)
    mb = alpaca_data.minute_bars(tickers, start, None, adjustment='raw', chunk=100)
    out = {}
    if mb.empty:
        return out
    mb = mb.sort_values('t')
    ts = mb['t'].dt.tz_convert(alpaca_data.NY)
    hm = ts.dt.hour * 60 + ts.dt.minute
    mb = mb[(hm >= 570) & (hm <= 960) & (ts.dt.strftime('%Y-%m-%d') == today)]
    for tk, g in mb.groupby('symbol', sort=False):
        f = g.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'})[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)
        f.index = pd.DatetimeIndex(g['t'].dt.tz_convert(alpaca_data.NY).values)
        out[tk] = f
    return out


def partial_bar(intraday):
    """오늘 1분봉 → 임시 일봉(시가·고가·저가·현재가·거래량). 봉이 없으면 None."""
    if intraday is None or len(intraday) == 0:
        return None
    return dict(Open=float(intraday['Open'].iloc[0]), High=float(intraday['High'].max()), Low=float(intraday['Low'].min()),
                Close=float(intraday['Close'].iloc[-1]), Volume=float(intraday['Volume'].sum()))


def preclose_scan(strat_str):
    """15:46 ET: 감시목록만 빠르게 확인. 캐시 일봉(어제까지)에 오늘 장중 임시봉을 붙여 check_signals 로 판정."""
    if not os.path.exists(WATCHLIST_PATH):
        print("  감시목록이 없음 → 먼저 마감 뒤 스캔(또는 --build-watchlist)"); return [], []
    try:
        wl = pd.read_csv(WATCHLIST_PATH, dtype={'ticker': str})
    except Exception:
        wl = pd.DataFrame(columns=['ticker', 'asof'])
    if wl.empty:
        print("  감시목록이 비어 있음"); return [], []
    now_et = _now_et(); today = now_et.strftime('%Y-%m-%d')
    if not is_us_trading_day(now_et.date()):
        print("  오늘은 미국 휴장일 → 스캔 생략"); return [], []
    hm_now = now_et.hour * 60 + now_et.minute
    if hm_now < PRECLOSE_WINDOW_ET[0]:
        print(f"  ET {now_et.strftime('%H:%M')} — 15:40 ET 전이라 마감 전 가격이 아님. 스캔 생략 (15:46 ET 에 실행)"); return [], []
    if hm_now > PRECLOSE_WINDOW_ET[1]:
        print("  15:48 ET 를 넘김 — 종가 주문(MOC) 마감 뒤라 오늘 마감 전 스캔은 생략"); return [], []
    prev_path = f"data/signal_{today}.csv"
    if os.path.exists(prev_path):
        try:
            prev = pd.read_csv(prev_path)
            if 'scan_type' in prev.columns and (prev['scan_type'] == 'preclose').any():
                print("  오늘 마감 전 스캔은 이미 실행됨 → 생략"); return [], []
        except Exception:
            pass
    if market_still_open(now_et) is False:
        print("  오늘은 장이 이미 끝남(조기 마감일 등) → 마감 전 스캔 생략"); return [], []
    tickers = sorted(wl['ticker'].dropna().unique().tolist())
    print(f"\n[마감 전 스캔] 감시목록 {len(tickers)}종목 · ET {now_et.strftime('%H:%M')} (데이터는 {PRECLOSE_DATA_LAG_MIN}분 전까지) · 기준일 {today}")
    if str(wl['asof'].dropna().max()) >= today:
        print("  ⚠ 감시목록 기준일이 오늘 이후 — 전날 목록이 아님. 그대로 진행하되 기록에 남김")
    hist = cache.load(days=HISTORY_DAYS, asof=today, symbols=set(tickers))
    hist = hist[hist['date'] < today]                      # 전날까지 확정 일봉만
    frames = cache.per_ticker(hist)
    intra = minute_bars_today(tickers, today)
    scan_time_kst = datetime.now(KST).strftime('%H:%M KST')
    signals = {'1': [], '3': []}
    run_1, run_3 = '1' in strat_str, '3' in strat_str
    n_checked = n_no_bar = n_short = 0
    for tk in tickers:
        try:
            df = frames.get(tk)
            if df is None or len(df) < 25:
                n_short += 1; continue
            bar = partial_bar(intra.get(tk))
            if bar is None:
                n_no_bar += 1; continue
            n_checked += 1
            df2 = pd.concat([df[['Open', 'High', 'Low', 'Close', 'Volume']], pd.DataFrame([bar], index=[pd.Timestamp(today)])])
            for strat, fields in check_signals(df2, run_1, run_3):
                rec = {'strategy': strat, 'ticker': tk, 'date': today, 'scan_time': scan_time_kst, 'scan_type': 'preclose',
                       'price_1530': bar['Close']}
                rec.update(fields); signals[strat].append(rec)
                print(f"    ★ [{strat}] {tk} @ ${bar['Close']:.2f}(15:30) | RSI7={fields['rsi7']:.1f} Intra={fields['intraday']:.0f}% Down={fields['consec_down']}d")
        except Exception as e:
            print(f"  Warning: {tk} preclose failed: {e}")
    print(f"  판정 {n_checked}종목 · 오늘 1분봉 없음 {n_no_bar}종목 · 일봉 부족 {n_short}종목")
    if tickers and n_checked == 0:
        print("  ⚠ 오늘 1분봉을 하나도 받지 못함 — 데이터 지연이거나 장이 열리지 않은 날")
    return signals['1'], signals['3']


# ─── Output ───────────────────────────────────────────────────────────────────

def print_results(signals_1, signals_3):
    all_sigs = signals_1 + signals_3
    date_str = all_sigs[0]['date'] if all_sigs else datetime.now(KST).strftime('%Y-%m-%d')
    for key, sigs, cond in (
        ('1', signals_1, 'Intra>20% + Ret3d<-15% + Down>5d + DistLow5<5% + RSI7<20'),
        ('3', signals_3, 'RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + Down>3d + DistLow5<3%'),
    ):
        cfg = STRATEGY_CONFIG[key]
        print(f"\n{'='*90}")
        print(f"  ★ {STRATEGY_NAMES[key]} — {date_str}")
        print(f"  조건: {cond}")
        print(f"  매매: 신호일 마감가 진입 | +{cfg['tp_pct']*100:.0f}% 익절 | {cfg['sl_pct']*100:.0f}% 손절 | {cfg['max_hold']}일")
        print(f"{'='*90}")
        if not sigs:
            print("  신호 없음")
            continue
        print(f"{'Ticker':<8} {'종가':>8} {'RSI7':>6} {'일중%':>7} {'1일%':>7} {'3일%':>7} {'연속↓':>5} {'Low5%':>7}")
        print("-" * 90)
        for s in sigs:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi7']:>6.1f} {s['intraday']:>6.1f}% "
                  f"{s['ret1d']:>6.1f}% {s['ret3d']:>6.1f}% {s['consec_down']:>5}d {s['dist_low5']:>6.1f}%")
        print(f"  Total: {len(sigs)} signals")


def dedupe_prefer_preclose(df):
    """같은 (전략, 종목, 날짜) 신호가 두 번 있으면 마감 전(preclose) 신호를 남긴다.
    파일에 어떤 순서로 들어 있든(마감 뒤 신호가 먼저 저장된 경우 포함) 결과가 같도록 안정 정렬 뒤 중복 제거."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df['strategy'] = df['strategy'].astype(str)
    pre = (df['scan_type'].astype(str) == 'preclose') if 'scan_type' in df.columns else pd.Series(False, index=df.index)
    df['_pre'] = pre.astype(int)
    df = df.sort_values('_pre', ascending=False, kind='stable')
    df = df.drop_duplicates(subset=['strategy', 'ticker', 'date'], keep='first')
    return df.drop(columns=['_pre']).sort_index().reset_index(drop=True)


def save_results(signals_1, signals_3, date_str=None, mode='afterclose'):
    os.makedirs('data', exist_ok=True)
    all_signals = signals_1 + signals_3
    if date_str is None:
        date_str = all_signals[0]['date'] if all_signals else get_expected_trading_date().strftime('%Y-%m-%d')

    daily_path = f"data/signal_{date_str}.csv"
    if all_signals:
        new = pd.DataFrame(all_signals)
        if os.path.exists(daily_path):
            try:
                old = pd.read_csv(daily_path, dtype={'strategy': str})
                if len(old) and 'ticker' in old.columns:
                    new = dedupe_prefer_preclose(pd.concat([old, new], ignore_index=True))   # 마감 전 신호 우선 유지
            except Exception:
                pass
        new.to_csv(daily_path, index=False)
        print(f"\n  저장: {daily_path} ({len(new)}건)")
    else:
        if os.path.exists(daily_path):
            print(f"\n  신호 없음 — 기존 {daily_path} 는 그대로 둠")
        else:
            pd.DataFrame(columns=['strategy', 'ticker', 'date', 'price', 'scan_type']).to_csv(daily_path, index=False)
            print(f"\n  저장: {daily_path} (신호 없음)")

    hist_path = "data/history.csv"
    if all_signals:
        df_new = pd.DataFrame(all_signals)
        if os.path.exists(hist_path):
            df_hist = pd.read_csv(hist_path, dtype={'strategy': str})
            df_hist = dedupe_prefer_preclose(pd.concat([df_hist, df_new], ignore_index=True))   # 마감 전 신호 우선 유지
        else:
            df_hist = df_new
        df_hist.to_csv(hist_path, index=False)
        print(f"  히스토리: {hist_path} ({len(df_hist)}건 누적)")

    dq = []
    for s in all_signals:
        if s.get('price', 0) <= 0:
            dq.append(f"  ⚠ DQ: [{s['strategy']}] {s['ticker']} — price={s.get('price')}")
        if not (0 <= s.get('rsi7', 50) <= 100):
            dq.append(f"  ⚠ DQ: [{s['strategy']}] {s['ticker']} — rsi7={s.get('rsi7')}")
    print("\n" + ("\n".join(dq) if dq else "  ✓ Data quality check passed — no issues found"))

    # 요약은 '그날 파일 전체'(마감 전 + 마감 뒤 합친 것) 기준
    try:
        merged = pd.read_csv(daily_path, dtype={'strategy': str})
        n1 = int((merged['strategy'] == '1').sum()) if 'strategy' in merged else 0
        n3 = int((merged['strategy'] == '3').sum()) if 'strategy' in merged else 0
        npre = int((merged.get('scan_type', pd.Series(dtype=str)) == 'preclose').sum()) if 'scan_type' in merged.columns else 0
    except Exception:
        n1, n3, npre = len(signals_1), len(signals_3), 0
    summary = {
        'scan_date': date_str,
        'scan_time': datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST'),
        'strategy_1_count': n1,
        'strategy_3_count': n3,
        'preclose_count': npre,
        'total_count': n1 + n3,
        'dq_warnings': len(dq),
        'version': 'v6',
        'data_source': 'alpaca',
        'mode': mode,
    }
    with open('data/latest_scan.json', 'w') as f:
        json.dump(summary, f, indent=2)
    return daily_path


# ─── 캐시 갱신 ────────────────────────────────────────────────────────────────

def update_cache(through, retries=3, wait_sec=120):
    """캐시를 through(끝난 거래일)까지 채운다. 그날 데이터가 아직 안 들어왔으면 wait_sec 쉬고 다시 (최대 retries 회)."""
    for attempt in range(retries):
        s = cache.update(through)
        print(f"  캐시: 종목 {s['universe']} · 추가 {len(s['days'])}일/{s['rows']}행 · 분할 +{s['splits_added']} · 정리 {len(s['pruned'])}일 · 마지막 {cache.last_cached_day()}")
        if cache.last_cached_day() and cache.last_cached_day() >= through:
            return True
        if attempt < retries - 1:
            print(f"  ⏳ {through} 일봉이 아직 없음 → {wait_sec // 60}분 뒤 다시 ({attempt + 1}/{retries})")
            time.sleep(wait_sec)
    print(f"  ⚠ {through} 일봉을 받지 못함 — 있는 데이터({cache.last_cached_day()})로 진행")
    return False


def rebuild_cache(through):
    for p in glob.glob(os.path.join(cache.CACHE_DIR, '????-??-??.parquet')):
        os.remove(p)
    print("  캐시 비움 → 처음부터 다시 받음")
    return update_cache(through, retries=1)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='US Stock Surge Scanner (Strategy 1 + 3)')
    parser.add_argument('--strategy', default=''.join(ACTIVE_STRATEGIES), help="'1', '3' or '13' (default)")
    parser.add_argument('--build-watchlist', action='store_true', help='캐시로 감시목록만 만들고 종료 (네트워크 없음)')
    parser.add_argument('--preclose', action='store_true', help='15:46 ET 감시목록 빠른 확인 (마감가 매수용 신호)')
    parser.add_argument('--rebuild-cache', action='store_true', help='일봉 캐시를 지우고 Alpaca 에서 처음부터 다시 받음')
    parser.add_argument('--force', action='store_true', help="오늘 이미 스캔했어도 다시 실행 (점검용)")
    args = parser.parse_args()
    strat_str = args.strategy

    t0 = time.time()
    print("=" * 80)
    print("  US Stock Surge Scanner — Strategy 1 + 3 (v6 · Alpaca)")
    print(f"  Mode: {'watchlist' if args.build_watchlist else 'preclose' if args.preclose else 'after-close full scan'} · Strategy: {strat_str}")
    print(f"  Scan Time: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    if args.build_watchlist:
        afterclose_scan(strat_str, write_watchlist=True)
        print(f"\n  완료: {time.time() - t0:.0f}초"); return

    if not alpaca_data.have_keys():
        print("  ✗ Alpaca 키가 없음 — GitHub 저장소 Settings → Secrets and variables → Actions 에 "
              "ALPACA_KEY / ALPACA_SECRET 을 등록하세요 (무료 Basic 플랜). 데이터는 바꾸지 않고 종료합니다.")
        raise SystemExit(1)

    if args.preclose:
        now_et = _now_et()
        if not is_us_trading_day(now_et.date()):
            print("  오늘은 미국 휴장일 → 마감 전 스캔 생략 (파일도 만들지 않음)"); return
        s1, s3 = preclose_scan(strat_str)
        print_results(s1, s3); save_results(s1, s3, date_str=now_et.strftime('%Y-%m-%d'), mode='preclose')
        print(f"\n  마감 전 스캔 완료: {time.time() - t0:.0f}초 | 1: {len(s1)}건 | 3: {len(s3)}건"); return

    # 언제 돌려도 안전하다: '마지막으로 확정된 거래일'(16:20 ET 전이면 어제)까지만 받아서 판정하므로
    # 장중에 돌려도 오늘의 미완성 가격이 종가처럼 기록되지 않는다. 같은 날을 두 번 스캔하지는 않는다.
    through = _last_session_et()
    if already_scanned(through) and not args.rebuild_cache and not args.force:
        print(f"  {through} 마감 뒤 스캔은 이미 끝남 (예약이 여러 번 걸려 있어 두 번째 실행) → 종료"); return
    print(f"\n[1] 일봉 캐시 갱신 (~{through})")
    if args.rebuild_cache:
        rebuild_cache(through)
    else:
        update_cache(through)

    print("\n[2] 전 종목 판정 + 내일 감시목록")
    signals_1, signals_3, asof = afterclose_scan(strat_str, write_watchlist=True)

    print_results(signals_1, signals_3)
    save_results(signals_1, signals_3, date_str=asof or through)
    try:
        open('/tmp/scanned', 'w').write(through)      # 워크플로: 실제로 스캔했을 때만 추적기를 돌린다
    except OSError:
        pass
    print(f"\n  스캔 완료: {time.time() - t0:.0f}초 | Strategy 1: {len(signals_1)}건 | 3: {len(signals_3)}건")


if __name__ == '__main__':
    main()
