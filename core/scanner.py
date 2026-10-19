#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Strategy 1 + 3 (v5)

  Strategy 1 (급락반등):
    신호: Intra>20% + Ret3d<-15% + ConsecDown>5 + DistLow5<5% + RSI7<20
  Strategy 3 (과매도):
    신호: RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + ConsecDown>3 + DistLow5<3%

  공통 매매 규칙 (v5, 실제로 체결 가능한 방식):
    진입: 신호일 다음 거래일 시가 (스캔이 장 마감 뒤에 돌기 때문)
    청산: +5% 익절 | -20% 손절 (갭으로 뚫리면 시가 체결) | 5거래일 타임아웃
    비용: 왕복 COST_PCT (shared_config) 를 결과에서 차감
    제외: ETF·ETN·레버리지 상품, 테스트 종목, 실제 주가 $1 미만, 20일 평균 거래량 1만주 미만

  ※ 과거 '백테스트 승률 90%' 표기는 신호일 종가 진입 + 비용 0 가정이었고,
    무작위 고변동 종목과 차이가 없었다(research/STRATEGY_REVIEW_v5.md). 화면에는
    실제 추적 성적만 표시한다.

  Usage:
    python scanner.py                 # 1 + 3
    python scanner.py --strategy 1    # 1 only
    python scanner.py --strategy 3    # 3 only

  Output: data/signal_YYYY-MM-DD.csv + data/history.csv (append) + data/latest_scan.json
================================================================================
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
import warnings
import os
import json
import argparse
from datetime import datetime, timezone, timedelta

from shared_config import (
    KST,
    MIN_PRICE,
    MIN_VOLUME,
    BATCH_SIZE,
    BATCH_DELAY,
    STRATEGY_CONFIG,
    STRATEGY_NAMES,
    ACTIVE_STRATEGIES,
    calc_rsi_wilder,
    get_all_tickers,
    download_batch,
    extract_ticker_df,
    get_expected_trading_date,
    is_us_trading_day,
    is_us_dst,
)

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


# ─── yfinance 데이터 갱신 대기 ────────────────────────────────────────────────

def wait_for_market_data(max_retries=10, retry_interval=180):
    """
    yfinance 종가 데이터가 당일 거래일로 업데이트될 때까지 대기.
    DST를 고려한 미국 장마감 시각 판단:
    - EDT (summer): 장마감 = UTC 20:00 → 데이터 기대 = UTC 20:30 이후
    - EST (winter): 장마감 = UTC 21:00 → 데이터 기대 = UTC 21:30 이후
    최대 10회 × 3분 = 30분 대기. (yfinance 데이터 갱신 지연 대응)
    """
    print(f"\n{'='*80}")
    print(f"  [사전 검증] yfinance 종가 데이터 갱신 확인")
    print(f"{'='*80}")

    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date()

    # DST 여부에 따라 장마감 시각 결정
    if is_us_dst(today_utc):
        close_hour, close_min = 20, 30   # EDT: UTC 20:30 이후
    else:
        close_hour, close_min = 21, 30   # EST: UTC 21:30 이후

    market_closed = (now_utc.hour > close_hour or
                     (now_utc.hour == close_hour and now_utc.minute >= close_min))

    # expected_date 결정: 공휴일 고려
    if market_closed and is_us_trading_day(today_utc):
        expected_date = today_utc
    else:
        expected_date = get_expected_trading_date()

    print(f"  UTC 시간: {now_utc.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"  기대 거래일: {expected_date}")

    for attempt in range(max_retries):
        try:
            spy = yf.download('SPY', period='5d', progress=False)
            if spy is not None and not spy.empty:
                # MultiIndex 처리
                if isinstance(spy.columns, pd.MultiIndex):
                    spy = spy.droplevel('Ticker', axis=1)
                latest_date = spy.index[-1].date()

                if latest_date >= expected_date:
                    print(f"  ✓ 최신 종가 확인 완료 (SPY 최신 거래일: {latest_date})")
                    return latest_date

                if attempt < max_retries - 1:
                    wait_min = retry_interval // 60
                    print(f"  ⏳ [{attempt+1}/{max_retries}] SPY 최신: {latest_date} → "
                          f"기대: {expected_date} | {wait_min}분 후 재확인...")
                    time.sleep(retry_interval)
                else:
                    print(f"  ⚠ {max_retries}회 재시도 후에도 미갱신.")
                    print(f"    최신 데이터({latest_date})로 진행합니다.")
                    return latest_date
        except Exception as e:
            print(f"  ⚠ SPY 데이터 확인 실패: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)

    print(f"  ⚠ 데이터 확인 불가. 스캔을 진행합니다.")
    return None


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
    """참고용 목표가/손절가 (신호일 종가 기준). 실제 추적은 다음날 시가 기준으로 다시 계산된다."""
    return round(price * (1 + STRATEGY_CONFIG['1']['tp_pct']), 2), round(price * (1 + STRATEGY_CONFIG['1']['sl_pct']), 2)


# ─── Phase 1: 공통 RSI 필터 ──────────────────────────────────────────────────

def phase1_rsi_filter(all_tickers):
    """30일 데이터로 RSI7 < 35 · 가격 · 거래량 조건을 통과한 후보만 추린다."""
    print(f"\n{'='*80}")
    print(f"  [Phase 1] 공통 필터링 (RSI7<{PHASE1_RSI7}, 가격>=${MIN_PRICE}, 20일 평균거래량>={MIN_VOLUME:,})")
    print(f"{'='*80}")
    print(f"  대상: {len(all_tickers)}개 종목")

    candidates = set()
    total_batches = (len(all_tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    for b_idx in range(0, len(all_tickers), BATCH_SIZE):
        batch = all_tickers[b_idx:b_idx + BATCH_SIZE]
        batch_num = b_idx // BATCH_SIZE + 1
        if batch_num % 20 == 1 or batch_num == total_batches:
            print(f"    Batch {batch_num}/{total_batches} ({len(candidates)} candidates)")

        data = download_batch(batch, period='30d')
        if data is None or data.empty:
            time.sleep(BATCH_DELAY)
            continue

        for tk in batch:
            try:
                df = extract_ticker_df(data, tk, len(batch))
                if df is None or len(df) < 10:
                    continue
                close = df['Close'].dropna()
                if len(close) < 10:
                    continue
                last_close = float(close.iloc[-1])
                avg_vol = float(df['Volume'].dropna().tail(20).mean())
                if last_close < MIN_PRICE or avg_vol < MIN_VOLUME:
                    continue
                rsi7 = calc_rsi_wilder(close, 7)
                if not pd.isna(rsi7.iloc[-1]) and float(rsi7.iloc[-1]) < PHASE1_RSI7:
                    candidates.add(tk)
            except Exception as e:
                print(f"  Warning: {tk} processing failed: {e}")
        time.sleep(BATCH_DELAY)

    candidates = sorted(candidates)
    print(f"  Phase 1 완료: {len(candidates)}개 후보")
    return candidates


# ─── Phase 2: 전략 1 · 3 정밀 판정 ───────────────────────────────────────────

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


def phase2_check_all(candidates, strat_str):
    print(f"\n{'='*80}")
    print(f"  [Phase 2] 정밀 분석 ({len(candidates)}개 후보)")
    print(f"{'='*80}")
    scan_time_kst = datetime.now(KST).strftime('%H:%M KST')
    run_1 = '1' in strat_str
    run_3 = '3' in strat_str
    signals = {'1': [], '3': []}

    total = (len(candidates) + 19) // 20
    for b_idx in range(0, len(candidates), 20):
        batch = candidates[b_idx:b_idx + 20]
        batch_num = b_idx // 20 + 1
        if batch_num % 10 == 1 or batch_num == total:
            print(f"    Phase2 Batch {batch_num}/{total} | 1:{len(signals['1'])} 3:{len(signals['3'])}")
        data = download_batch(batch, period='120d')
        if data is None or data.empty:
            time.sleep(BATCH_DELAY)
            continue
        for tk in batch:
            try:
                df = extract_ticker_df(data, tk, len(batch))
                if df is None or len(df) < 25:
                    continue
                trade_date = df.index[-1].strftime('%Y-%m-%d')
                for strat, fields in check_signals(df, run_1, run_3):
                    rec = {'strategy': strat, 'ticker': tk, 'date': trade_date, 'scan_time': scan_time_kst}
                    rec.update(fields)
                    signals[strat].append(rec)
                    print(f"    ★ [{strat}] {tk} @ ${fields['price']:.2f} | RSI7={fields['rsi7']:.1f} "
                          f"Intra={fields['intraday']:.0f}% Ret1d={fields['ret1d']:.1f}% "
                          f"Ret3d={fields['ret3d']:.1f}% Down={fields['consec_down']}d Dist5={fields['dist_low5']:.1f}%")
            except Exception as e:
                print(f"  Warning: {tk} phase2 failed: {e}")
        time.sleep(BATCH_DELAY)
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
        print(f"  매매: 다음날 시가 진입 | +{cfg['tp_pct']*100:.0f}% 익절 | {cfg['sl_pct']*100:.0f}% 손절 | {cfg['max_hold']}일")
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


def save_results(signals_1, signals_3):
    os.makedirs('data', exist_ok=True)
    all_signals = signals_1 + signals_3
    date_str = all_signals[0]['date'] if all_signals else datetime.now(KST).strftime('%Y-%m-%d')

    daily_path = f"data/signal_{date_str}.csv"
    if all_signals:
        new = pd.DataFrame(all_signals)
        if os.path.exists(daily_path):
            try:
                old = pd.read_csv(daily_path, dtype={'strategy': str})
                if len(old) and 'ticker' in old.columns:
                    new = pd.concat([old, new], ignore_index=True)
                    new['strategy'] = new['strategy'].astype(str)
                    new = new.drop_duplicates(subset=['strategy', 'ticker', 'date'], keep='first')   # 먼저 저장된(마감 전) 신호 유지
            except Exception:
                pass
        new.to_csv(daily_path, index=False)
        print(f"\n  저장: {daily_path} ({len(new)}건)")
    else:
        pd.DataFrame(columns=['strategy', 'ticker', 'date', 'price']).to_csv(daily_path, index=False)
        print(f"\n  저장: {daily_path} (신호 없음)")

    hist_path = "data/history.csv"
    if all_signals:
        df_new = pd.DataFrame(all_signals)
        if os.path.exists(hist_path):
            df_hist = pd.read_csv(hist_path)
            df_hist = pd.concat([df_hist, df_new], ignore_index=True)
            df_hist['strategy'] = df_hist['strategy'].astype(str)
            df_hist.drop_duplicates(subset=['strategy', 'ticker', 'date'], keep='last', inplace=True)
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

    summary = {
        'scan_date': date_str,
        'scan_time': datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST'),
        'strategy_1_count': len(signals_1),
        'strategy_3_count': len(signals_3),
        'total_count': len(all_signals),
        'dq_warnings': len(dq),
        'version': 'v5',
    }
    with open('data/latest_scan.json', 'w') as f:
        json.dump(summary, f, indent=2)
    return daily_path


# ─── Main ─────────────────────────────────────────────────────────────────────

# ─── v6: 두 단계 스캔 ─────────────────────────────────────────────────────────
#  (a) build-watchlist : 장 마감 뒤(또는 아무 때나) 전 종목을 훑어 '내일 신호가 날 수 있는' 종목만 저장 (느려도 됨)
#  (b) preclose        : 15:30 ET에 감시목록만 빠르게 받아(1~3분) 그 시점 가격으로 조건 판정 → 마감가 매수용 신호
#  (c) 기본(인자 없음) : 예전처럼 마감 뒤 전 종목 스캔 (기록·대조용). 같은 날 preclose 신호가 있으면 덮어쓰지 않고 뒤에 합침.
WATCHLIST_PATH = 'data/watchlist.csv'


def build_watchlist(all_tickers):
    """전날 데이터로 감시목록: RSI7 < WATCHLIST_RSI7, 연속하락 ≥ WATCHLIST_CONSEC, 가격·거래량 기준."""
    from shared_config import WATCHLIST_RSI7, WATCHLIST_CONSEC
    print(f"\n[감시목록] 전 종목 {len(all_tickers)}개 → RSI7<{WATCHLIST_RSI7} & 연속하락≥{WATCHLIST_CONSEC}")
    rows = []
    total_batches = (len(all_tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    for b_idx in range(0, len(all_tickers), BATCH_SIZE):
        batch = all_tickers[b_idx:b_idx + BATCH_SIZE]
        bn = b_idx // BATCH_SIZE + 1
        if bn % 20 == 1 or bn == total_batches:
            print(f"    Batch {bn}/{total_batches} ({len(rows)} 후보)")
        data = download_batch(batch, period='60d')
        if data is None or data.empty:
            time.sleep(BATCH_DELAY); continue
        for tk in batch:
            try:
                df = extract_ticker_df(data, tk, len(batch))
                if df is None or len(df) < 12:
                    continue
                close = df['Close'].dropna()
                last = float(close.iloc[-1]); avg_vol = float(df['Volume'].dropna().tail(20).mean())
                if last < MIN_PRICE * 0.9 or avg_vol < MIN_VOLUME:
                    continue
                rsi = calc_rsi_wilder(close, 7)
                if pd.isna(rsi.iloc[-1]) or float(rsi.iloc[-1]) >= WATCHLIST_RSI7:
                    continue
                if calc_consec_down(close) < WATCHLIST_CONSEC:
                    continue
                rows.append({'ticker': tk, 'asof': df.index[-1].strftime('%Y-%m-%d'), 'close': round(last, 4), 'rsi7': round(float(rsi.iloc[-1]), 1),
                             'consec_down': calc_consec_down(close), 'avg_vol20': int(avg_vol)})
            except Exception as e:
                print(f"  Warning: {tk}: {e}")
        time.sleep(BATCH_DELAY)
    wl = pd.DataFrame(rows)
    os.makedirs('data', exist_ok=True); wl.to_csv(WATCHLIST_PATH, index=False)
    print(f"  감시목록 저장: {WATCHLIST_PATH} ({len(wl)}종목, 기준일 {wl['asof'].max() if len(wl) else '-'})")
    return wl


def _today_partial_bar(tk, intraday):
    """오늘 장중 1분봉 → 임시 일봉(시가·고가·저가·현재가·거래량). 없으면 None."""
    if intraday is None or intraday.empty:
        return None
    return dict(Open=float(intraday['Open'].iloc[0]), High=float(intraday['High'].max()), Low=float(intraday['Low'].min()),
                Close=float(intraday['Close'].iloc[-1]), Volume=float(intraday['Volume'].sum()))


def preclose_scan(strat_str):
    """15:30 ET: 감시목록만 빠르게 확인. 일봉(60d)에 오늘 장중 임시봉을 붙여 check_signals 로 판정."""
    if not os.path.exists(WATCHLIST_PATH):
        print("  감시목록이 없음 → 먼저 --build-watchlist"); return [], []
    wl = pd.read_csv(WATCHLIST_PATH, dtype={'ticker': str})
    tickers = sorted(wl['ticker'].dropna().unique().tolist())
    now_et = datetime.now(timezone(timedelta(hours=-4 if is_us_dst(datetime.now(timezone.utc).date()) else -5)))
    today = now_et.strftime('%Y-%m-%d')
    print(f"\n[마감 전 스캔] 감시목록 {len(tickers)}종목 · ET {now_et.strftime('%H:%M')} · 기준일 {today}")
    if wl['asof'].max() >= today:
        print("  ⚠ 감시목록 기준일이 오늘 이후 — 전날 목록이 아님. 그대로 진행하되 기록에 남김")
    scan_time_kst = datetime.now(KST).strftime('%H:%M KST')
    signals = {'1': [], '3': []}
    run_1, run_3 = '1' in strat_str, '3' in strat_str
    for b_idx in range(0, len(tickers), 40):
        batch = tickers[b_idx:b_idx + 40]
        daily = download_batch(batch, period='60d')
        try:
            intra = yf.download(' '.join(batch), period='1d', interval='1m', group_by='ticker', progress=False, threads=True, prepost=False, timeout=30)
        except Exception as e:
            print(f"    1분봉 실패: {e}"); intra = None
        for tk in batch:
            try:
                df = extract_ticker_df(daily, tk, len(batch))
                if df is None or len(df) < 25:
                    continue
                df = df[df.index.strftime('%Y-%m-%d') < today]            # 전날까지 확정 일봉만
                itd = extract_ticker_df(intra, tk, len(batch)) if intra is not None else None
                bar = _today_partial_bar(tk, itd)
                if bar is None:
                    continue
                df2 = pd.concat([df, pd.DataFrame([bar], index=[pd.Timestamp(today)])])
                for strat, fields in check_signals(df2, run_1, run_3):
                    rec = {'strategy': strat, 'ticker': tk, 'date': today, 'scan_time': scan_time_kst, 'scan_type': 'preclose',
                           'price_1530': bar['Close']}
                    rec.update(fields); signals[strat].append(rec)
                    print(f"    ★ [{strat}] {tk} @ ${bar['Close']:.2f}(15:30) | RSI7={fields['rsi7']:.1f} Intra={fields['intraday']:.0f}% Down={fields['consec_down']}d")
            except Exception as e:
                print(f"  Warning: {tk} preclose failed: {e}")
        time.sleep(0.5)
    return signals['1'], signals['3']


def main():
    parser = argparse.ArgumentParser(description='US Stock Surge Scanner (Strategy 1 + 3)')
    parser.add_argument('--strategy', default=''.join(ACTIVE_STRATEGIES), help="'1', '3' or '13' (default)")
    parser.add_argument('--build-watchlist', action='store_true', help='전날 데이터로 감시목록만 만들고 종료')
    parser.add_argument('--preclose', action='store_true', help='15:30 ET 감시목록 빠른 확인 (마감가 매수용 신호)')
    args = parser.parse_args()
    strat_str = args.strategy

    t0 = time.time()
    print("=" * 80)
    print("  US Stock Surge Scanner — Strategy 1 + 3 (v6)")
    print(f"  Mode: {'watchlist' if args.build_watchlist else 'preclose' if args.preclose else 'after-close full scan'} · Strategy: {strat_str}")
    print(f"  Scan Time: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    if args.build_watchlist:
        build_watchlist(get_all_tickers())
        print(f"\n  완료: {time.time() - t0:.0f}초"); return

    if args.preclose:
        s1, s3 = preclose_scan(strat_str)
        print_results(s1, s3); save_results(s1, s3)
        print(f"\n  마감 전 스캔 완료: {time.time() - t0:.0f}초 | 1: {len(s1)}건 | 3: {len(s3)}건"); return

    confirmed_date = wait_for_market_data(max_retries=3, retry_interval=120)
    if confirmed_date:
        print(f"\n  확인된 거래일: {confirmed_date}")

    print("\n[1] 종목 수집 중...")
    all_tickers = get_all_tickers()
    print("\n[2] Phase 1: 공통 필터...")
    candidates = phase1_rsi_filter(all_tickers)
    print("\n[3] Phase 2: 전략 1·3 판정...")
    signals_1, signals_3 = phase2_check_all(candidates, strat_str)
    for s_ in signals_1 + signals_3:
        s_['scan_type'] = 'afterclose'

    print_results(signals_1, signals_3)
    save_results(signals_1, signals_3)
    print(f"\n  스캔 완료: {time.time() - t0:.0f}초 | Strategy 1: {len(signals_1)}건 | 3: {len(signals_3)}건")
    # 마감 뒤 스캔이 끝났으면 내일을 위한 감시목록도 갱신
    try:
        build_watchlist(all_tickers)
    except Exception as e:
        print(f"  감시목록 갱신 실패: {e}")


if __name__ == '__main__':
    main()
