#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Detection — Combined Scanner (Strategy A + B + C + D + E + F + G + H + I + J)

  Strategy A (+5% in 5 Days):
    매수: Intra>20% + Ret3d<-15% + ConsecDown>5 + DistLow5<5% + RSI7<20
    매도: +5% 익절 | -20% 손절 | 5일 타임아웃
    백테스트: 90.1% (236/262), 최적청산 누적 +515%

  Strategy B (+15% High-Gain):
    매수: RSI7<20 + RSI14<35 + ATR>3 + Intra>15% + MA20<=-25% + RevGrowth>0
    매도: +15% 지정가 | -20% 손절 | 10일 종가청산
    백테스트: 90.3% (28/31)

  Strategy C (+5% 2일 연속 급락 과매도 반등):
    매수: RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + ConsecDown>3 + DistLow5<3%
    매도: +5% 익절 | -20% 손절 | 5일 타임아웃
    백테스트: 86.9% (542/624), +7% 기준 81.4%

  Strategy D (+20% 초저가 폭락 반등):
    매수: Price<=$3 + Ret5d<=-40% + Intra>=30% + RSI14<=25
    매도: +20% 익절 | 30일 타임아웃
    백테스트: 97.7% (127/130), 건당 평균 +18.9%, 중간 도달일 2일

  Strategy E (+10% 저가주 급락 반등):
    매수: $3~$10 + Ret5d<=-25% + Intra>=20% + ConsecDown>=5 + AvgVol>=20만
    매도: +10% 익절 | 30일 타임아웃
    백테스트: 91.0% (273/300), 건당 평균 max +104.9%, 중간 도달일 2일

  Strategy F (+50% 바닥 급등):
    매수: Vol20d>10% + Dist52wHigh<-85% + BB%B<0 + Ret1d<-5% + GapUp>5%
    매도: +50% 익절 | -20% 손절 | 20일 타임아웃
    백테스트: 87.5% (8건/5년)

  Strategy G (+40% MACD 전환 급등):
    매수: Vol20d>10% + RSI7<30 + MACD 골든크로스 + GapUp>5% + Price<SMA5
    매도: +40% 익절 | -20% 손절 | 20일 타임아웃
    백테스트: 90.0% (10건/5년)

  Strategy H (+40% ATR 확대 급등):
    매수: RSI7<30 + 52wHigh<-85% + Ret1d<-5% + GapUp>5% + ATR확대>25%
    매도: +40% 익절 | -20% 손절 | 20일 타임아웃
    백테스트: 90.0% (10건/5년)

  Strategy I (+10% 과매도 반등 단타):
    매수: RSI14<30 + Vol2x + MACD>0 + Ret1d<-5% + Ret5d>0
    매도: +10% 익절 | -20% 손절 | 5일 타임아웃
    백테스트: 100% (8건/5년)

  Strategy J (+10% MACD 전환 단타):
    매수: Vol20d>10% + RSI14<30 + Vol2x + MACD cross up + Stoch<20
    매도: +10% 익절 | -20% 손절 | 5일 타임아웃
    백테스트: 90% (9/10, 5년)

  Usage:
    python scanner.py                        # Scan all strategies (A+B+C+D+E+F+G+H+I+J)
    python scanner.py --strategy A           # Strategy A only
    python scanner.py --strategy DEF         # Strategy D + E + F only
    python scanner.py --strategy ABCDEFGHIJ  # All strategies

  Output: data/signal_YYYY-MM-DD.csv + data/history.csv (append)
================================================================================
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import time
import warnings
import os
import json
import argparse
from datetime import datetime, timedelta, timezone

# Import from shared_config
from shared_config import (
    KST,
    LEVERAGED_ETF,
    MIN_PRICE,
    MIN_VOLUME,
    BATCH_SIZE,
    BATCH_DELAY,
    STRATEGY_CONFIG,
    calc_rsi_wilder,
    get_all_tickers,
    download_batch,
    extract_ticker_df,
    get_expected_trading_date,
    is_us_trading_day,
    is_us_dst,
)

warnings.filterwarnings('ignore')

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

# ─── Configuration ────────────────────────────────────────────────────────────

# === Strategy A thresholds (+5%) ===
A_INTRA_THRESH   = 0.20     # 일중 변동폭 > 20%
A_RET3D_THRESH   = -0.15    # 3일 수익률 < -15%
A_CONSEC_DOWN    = 5         # 연속 하락 > 5일
A_DIST_LOW5      = 0.05      # 5일 최저가 대비 < 5%
A_RSI7_THRESH    = 20        # RSI(7) < 20
A_TAKE_PROFIT    = 0.05      # +5%
A_STOP_LOSS      = -0.20     # -20%
A_MAX_HOLD_DAYS  = 5

# === Strategy B thresholds (+15%) ===
B_RSI7_THRESH      = 20
B_RSI14_THRESH     = 35
B_ATR_RATIO_THRESH = 3.0
B_INTRA_THRESH     = 0.15
B_MA20_THRESH      = -0.25
B_TAKE_PROFIT      = 0.15    # +15%
B_STOP_LOSS        = -0.20   # -20%
B_MAX_HOLD_DAYS    = 10

# === Strategy C thresholds (+5% 과매도 반등) ===
C_RSI7_THRESH    = 30        # RSI(7) < 30
C_INTRA_THRESH   = 0.20      # 일중 변동폭 > 20%
C_RET1D_THRESH   = -0.08     # 당일 수익률 < -8%
C_CONSEC_DOWN    = 3          # 연속 하락 > 3일 (4일 이상)
C_DIST_LOW5      = 0.03       # 5일 최저가 대비 < 3%
C_TAKE_PROFIT    = 0.05       # +5%
C_STOP_LOSS      = -0.20      # -20%
C_MAX_HOLD_DAYS  = 5

# === Strategy D thresholds (+20% 초저가 폭락 반등) ===
D_PRICE_THRESH   = 3.0        # 종가 <= $3
D_RET5D_THRESH   = -0.40      # 5일 수익률 <= -40%
D_INTRA_THRESH   = 0.30       # 일중 변동폭 >= 30%
D_RSI14_THRESH   = 25         # RSI(14) <= 25
D_TAKE_PROFIT    = 0.20       # +20%
D_MAX_HOLD_DAYS  = 30

# === Strategy E thresholds (+10% 저가주 급락 반등) ===
E_PRICE_MIN      = 3.0        # 종가 >= $3
E_PRICE_MAX      = 10.0       # 종가 <= $10
E_RET5D_THRESH   = -0.25      # 5일 수익률 <= -25%
E_INTRA_THRESH   = 0.20       # 일중 변동폭 >= 20%
E_CONSEC_DOWN    = 5           # 연속 하락 >= 5일
E_VOL_MIN        = 200000     # 20일 평균 거래량 >= 20만주
E_TAKE_PROFIT    = 0.10       # +10%
E_MAX_HOLD_DAYS  = 30

# === Strategy F thresholds (+50% 바닥 급등) ===
F_VOLATILITY_20D  = 0.10      # 20일 변동성 > 10%
F_DIST_52W_HIGH   = -0.85     # 52주 고점 대비 -85% 이하
F_BB_PCTB_THRESH  = 0.0       # 볼린저 %B < 0
F_RET1D_THRESH    = -0.05     # 전일 수익률 < -5%
F_GAP_UP_THRESH   = 0.05      # 갭업 > 5%
F_TAKE_PROFIT     = 0.50      # +50%
F_STOP_LOSS       = -0.20     # -20%
F_MAX_HOLD_DAYS   = 20

# === Strategy G thresholds (+40% MACD 전환 급등) ===
G_VOLATILITY_20D  = 0.10
G_RSI7_THRESH     = 30
G_GAP_UP_THRESH   = 0.05
G_TAKE_PROFIT     = 0.40
G_STOP_LOSS       = -0.20
G_MAX_HOLD_DAYS   = 20

# === Strategy H thresholds (+40% ATR 확대 급등) ===
H_RSI7_THRESH       = 30
H_DIST_52W_HIGH     = -0.85
H_RET1D_THRESH      = -0.05
H_GAP_UP_THRESH     = 0.05
H_ATR_CHANGE_THRESH = 0.25
H_TAKE_PROFIT       = 0.40
H_STOP_LOSS         = -0.20
H_MAX_HOLD_DAYS     = 20

# === Strategy I thresholds (+10% 과매도 반등 단타) ===
I_RSI14_THRESH     = 30
I_VOL_RATIO_THRESH = 2.0
I_MACD_POS         = True        # macd_hist > 0
I_RET1D_THRESH     = -0.05       # prev_big_drop: return_1d < -5%
I_RET5D_THRESH     = 0.0         # ret5d_pos: return_5d > 0
I_TAKE_PROFIT      = 0.10        # +10%
I_STOP_LOSS        = -0.20       # -20%
I_MAX_HOLD_DAYS    = 5

# === Strategy J thresholds (+10% MACD 전환 단타) ===
J_VOLATILITY_20D   = 0.10        # very_high_vol: volatility_20d > 10%
J_RSI14_THRESH     = 30
J_VOL_RATIO_THRESH = 2.0
J_MACD_CROSSUP     = True        # macd_cross_up: hist < 0 yesterday, > 0 today
J_STOCH_OVERSOLD   = 20          # stoch_k < 20
J_TAKE_PROFIT      = 0.10        # +10%
J_STOP_LOSS        = -0.20       # -20%
J_MAX_HOLD_DAYS    = 5

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


# ─── Unified Phase 1: 공통 RSI 필터 ─────────────────────────────────────────

def phase1_rsi_filter(all_tickers, strat_str):
    """Phase 1: 공통 필터링 (A/B/C: RSI7<35, D: Price<=$3, E: Price $3~$10, F/G/H/I/J: Vol>6%)
    한 번의 30d 스캔으로 모든 전략 후보를 수집.
    """
    run_d = 'D' in strat_str
    run_e = 'E' in strat_str
    run_f = 'F' in strat_str
    run_g = 'G' in strat_str
    run_h = 'H' in strat_str
    run_i = 'I' in strat_str
    run_j = 'J' in strat_str
    run_abc = any(s in strat_str for s in ['A', 'B', 'C'])

    print(f"\n{'='*80}")
    print(f"  [Phase 1] 공통 필터링 (RSI7<35{' + Price<=$3' if run_d else ''}{' + Price $3~$10' if run_e else ''}{' + Vol>6%' if (run_f or run_g or run_h or run_i or run_j) else ''})")
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

                # A/B/C 후보: RSI7 < 35
                if run_abc:
                    rsi7 = calc_rsi_wilder(close, 7)
                    if not pd.isna(rsi7.iloc[-1]) and float(rsi7.iloc[-1]) < 35:
                        candidates.add(tk)

                # D 후보: Price <= $3 (추가 다운로드 없이 동일 데이터 사용)
                if run_d and last_close <= D_PRICE_THRESH:
                    candidates.add(tk)

                # E 후보: Price $3~$10 + AvgVol >= 20만
                if run_e and E_PRICE_MIN <= last_close <= E_PRICE_MAX and avg_vol >= E_VOL_MIN:
                    candidates.add(tk)

                # F/G/H/I/J 후보: 변동성 높거나 52주 최저 근처 (느슨한 필터, 실제 기준은 0.10)
                if run_f or run_g or run_h or run_i or run_j:
                    daily_ret = close.pct_change()
                    vol_20d = float(daily_ret.tail(20).std()) if len(daily_ret) >= 20 else 0
                    if vol_20d > 0.06:
                        candidates.add(tk)
            except Exception as e:
                print(f"  Warning: {tk} processing failed: {e}")
                continue
        time.sleep(BATCH_DELAY)

    candidates = sorted(candidates)
    print(f"  Phase 1 완료: {len(candidates)}개 후보")
    return candidates


# ─── Unified Phase 2: 한 번의 다운로드로 A/B/C/D/E 동시 체크 ────────────────────

def phase2_check_all(candidates, strat_str):
    """Phase 2: 120d(또는 2y if F/G/H) 데이터를 한 번만 받아서 A/B/C/D/E/F/G/H/I/J 조건을 모두 체크"""
    print(f"\n{'='*80}")
    print(f"  [Phase 2] 정밀 분석 ({len(candidates)}개 후보)")
    print(f"{'='*80}")

    scan_time_kst = datetime.now(KST).strftime('%H:%M KST')

    signals_a = []
    signals_b = []
    signals_c = []
    signals_d = []
    signals_e = []
    signals_f = []
    signals_g = []
    signals_h = []
    signals_i = []
    signals_j = []

    run_a = 'A' in strat_str
    run_b = 'B' in strat_str
    run_c = 'C' in strat_str
    run_d = 'D' in strat_str
    run_e = 'E' in strat_str
    run_f = 'F' in strat_str
    run_g = 'G' in strat_str
    run_h = 'H' in strat_str
    run_i = 'I' in strat_str
    run_j = 'J' in strat_str

    for b_idx in range(0, len(candidates), 20):
        batch = candidates[b_idx:b_idx + 20]
        batch_num = b_idx // 20 + 1
        total_p2 = (len(candidates) + 19) // 20

        if batch_num % 10 == 1 or batch_num == total_p2:
            print(f"    Phase2 Batch {batch_num}/{total_p2} | "
                  f"A:{len(signals_a)} B:{len(signals_b)} C:{len(signals_c)} D:{len(signals_d)} E:{len(signals_e)} F:{len(signals_f)} G:{len(signals_g)} H:{len(signals_h)} I:{len(signals_i)} J:{len(signals_j)}")

        # 120d로 한 번만 다운로드 (B가 MA20에 120d 필요, F/G/H는 252d=2y 필요)
        data = download_batch(batch, period='2y' if (run_f or run_g or run_h) else '120d')
        if data is None or data.empty:
            time.sleep(BATCH_DELAY)
            continue

        for tk in batch:
            try:
                df = extract_ticker_df(data, tk, len(batch))
                if df is None or len(df) < 25:
                    continue

                close = df['Close'].astype(float)
                high  = df['High'].astype(float)
                low   = df['Low'].astype(float)
                opn   = df['Open'].astype(float)
                vol   = df['Volume'].astype(float)
                n = len(close)

                c_last = float(close.iloc[-1])
                h_last = float(high.iloc[-1])
                l_last = float(low.iloc[-1])
                o_last = float(opn.iloc[-1])
                trade_date = df.index[-1].strftime('%Y-%m-%d')  # 실제 미국 거래일

                if o_last <= 0 or c_last < MIN_PRICE:
                    continue

                avg_vol = float(vol.tail(20).mean())
                if avg_vol < MIN_VOLUME:
                    continue

                # ── 공통 계산 (한 번만) ──
                intra = (h_last - l_last) / l_last  # FIX #4: low-based for consistency with backtest
                consec = calc_consec_down(close)

                low5_min = float(low.iloc[-5:].min()) if n >= 5 else None
                dist_low5 = (c_last - low5_min) / max(low5_min, 0.01) if low5_min else None

                # ── RSI7 계산: A/B/C/G/H 필요, D/E는 필요 없음 ──
                rsi7_val = None
                if run_a or run_b or run_c or run_g or run_h:
                    rsi7 = calc_rsi_wilder(close, 7)
                    if not pd.isna(rsi7.iloc[-1]):
                        rsi7_val = float(rsi7.iloc[-1])
                    else:
                        # RSI7이 NaN인 경우: A/B/C/G/H는 스킵, D/E는 계속
                        if run_a or run_b or run_c or run_g or run_h:
                            if not (run_d or run_e):
                                continue
                            # A/B/C/G/H를 못하지만 D/E는 계속하기 위해 pass

                # ══════════════════════════════════════════════
                # Strategy A: Intra>20% + Ret3d<-15% + Down>5 + DistLow5<5% + RSI7<20
                # ══════════════════════════════════════════════
                if run_a and rsi7_val is not None and rsi7_val < A_RSI7_THRESH:
                    if (intra > A_INTRA_THRESH and
                        n >= 4 and
                        consec > A_CONSEC_DOWN and
                        dist_low5 is not None and dist_low5 < A_DIST_LOW5):

                        ret3d = c_last / float(close.iloc[-4]) - 1
                        if ret3d < A_RET3D_THRESH:
                            tp_price = round(c_last * (1 + A_TAKE_PROFIT), 2)
                            sl_price = round(c_last * (1 + A_STOP_LOSS), 2)
                            signals_a.append({
                                'strategy': 'A',
                                'ticker': tk,
                                'date': trade_date,
                                'scan_time': scan_time_kst,
                                'price': round(c_last, 2),
                                'rsi7': round(rsi7_val, 1),
                                'intraday': round(intra * 100, 1),
                                'ret3d': round(ret3d * 100, 1),
                                'consec_down': consec,
                                'dist_low5': round(dist_low5 * 100, 1),
                                'tp_price': tp_price,
                                'sl_price': sl_price,
                            })
                            print(f"    ★ [A] {tk} @ ${c_last:.2f} | RSI7={rsi7_val:.1f} "
                                  f"Intra={intra*100:.0f}% Ret3d={ret3d*100:.1f}% "
                                  f"Down={consec}d Dist={dist_low5*100:.1f}%")

                # ══════════════════════════════════════════════
                # Strategy B: RSI7<20 + RSI14<35 + ATR>3 + Intra>15% + MA20<=-25% + RevGrowth>0
                # ══════════════════════════════════════════════
                if run_b and rsi7_val is not None and rsi7_val < B_RSI7_THRESH and intra > B_INTRA_THRESH:
                    # ATR ratio
                    tr_arr = np.maximum(
                        high.values - low.values,
                        np.maximum(
                            np.abs(high.values - np.roll(close.values, 1)),
                            np.abs(low.values - np.roll(close.values, 1))
                        )
                    )
                    tr_arr[0] = high.values[0] - low.values[0]
                    tr_s = pd.Series(tr_arr, index=close.index)
                    atr5 = tr_s.rolling(5).mean()
                    atr20 = tr_s.rolling(20).mean()
                    atr_ratio = float(atr5.iloc[-1]) / max(float(atr20.iloc[-1]), 0.001)

                    if not pd.isna(atr_ratio) and atr_ratio > B_ATR_RATIO_THRESH:
                        # MA20
                        ma20 = close.rolling(20).mean()
                        ma20_val = float(ma20.iloc[-1])
                        if not pd.isna(ma20_val) and ma20_val > 0:
                            ma20_pos = (c_last - ma20_val) / ma20_val
                            if ma20_pos <= B_MA20_THRESH:
                                # RSI14
                                rsi14 = calc_rsi_wilder(close, 14)
                                rsi14_val = float(rsi14.iloc[-1])
                                if not pd.isna(rsi14_val) and rsi14_val < B_RSI14_THRESH:
                                    # RevGrowth (API 호출 — 가장 마지막에)
                                    try:
                                        info = yf.Ticker(tk).info
                                        time.sleep(0.3)  # FIX #3: avoid rate limiting
                                        rev_growth = info.get('revenueGrowth', None)
                                        if rev_growth is not None and rev_growth > 0:
                                            tp_price = round(c_last * (1 + B_TAKE_PROFIT), 2)
                                            sl_price = round(c_last * (1 + B_STOP_LOSS), 2)
                                            signals_b.append({
                                                'strategy': 'B',
                                                'ticker': tk,
                                                'date': trade_date,
                                                'scan_time': scan_time_kst,
                                                'price': round(c_last, 2),
                                                'rsi7': round(rsi7_val, 1),
                                                'rsi14': round(rsi14_val, 1),
                                                'atr_ratio': round(atr_ratio, 2),
                                                'intra_pct': round(intra * 100, 1),
                                                'ma20_pos': round(ma20_pos * 100, 1),
                                                'rev_growth': round(rev_growth * 100, 1),
                                                'tp_price': tp_price,
                                                'sl_price': sl_price,
                                            })
                                            print(f"    ★ [B] {tk} @ ${c_last:.2f} | RSI7={rsi7_val:.1f} "
                                                  f"RSI14={rsi14_val:.1f} ATR={atr_ratio:.1f} "
                                                  f"Intra={intra*100:.0f}% MA20={ma20_pos*100:.1f}%")
                                    except Exception as e:
                                        print(f"    Warning: {tk} info fetch failed: {e}")

                # ══════════════════════════════════════════════
                # Strategy C: RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + Down>3 + DistLow5<3%
                # ══════════════════════════════════════════════
                if run_c and rsi7_val is not None and rsi7_val < C_RSI7_THRESH and intra > C_INTRA_THRESH:
                    if (n >= 3 and
                        consec > C_CONSEC_DOWN and
                        dist_low5 is not None and dist_low5 < C_DIST_LOW5):

                        ret1d = (c_last - float(close.iloc[-2])) / max(float(close.iloc[-2]), 0.01)
                        prev_close = float(close.iloc[-2])
                        prev2_close = float(close.iloc[-3])

                        if ret1d < C_RET1D_THRESH and prev_close < prev2_close:
                            tp_price = round(c_last * (1 + C_TAKE_PROFIT), 2)
                            sl_price = round(c_last * (1 + C_STOP_LOSS), 2)
                            signals_c.append({
                                'strategy': 'C',
                                'ticker': tk,
                                'date': trade_date,
                                'scan_time': scan_time_kst,
                                'price': round(c_last, 2),
                                'rsi7': round(rsi7_val, 1),
                                'intraday': round(intra * 100, 1),
                                'ret1d': round(ret1d * 100, 1),
                                'consec_down': consec,
                                'dist_low5': round(dist_low5 * 100, 2),
                                'tp_price': tp_price,
                                'sl_price': sl_price,
                            })
                            print(f"    ★ [C] {tk} @ ${c_last:.2f} | RSI7={rsi7_val:.1f} "
                                  f"Intra={intra*100:.0f}% Ret1d={ret1d*100:.1f}% "
                                  f"Down={consec}d Dist5={dist_low5*100:.1f}%")

                # ══════════════════════════════════════════════
                # Strategy D: Price<=$3 + Ret5d<=-40% + Intra>=30% + RSI14<=25
                # ══════════════════════════════════════════════
                if run_d and c_last <= D_PRICE_THRESH and intra >= D_INTRA_THRESH:
                    if n >= 6:
                        ret5d = c_last / float(close.iloc[-6]) - 1
                        if ret5d <= D_RET5D_THRESH:
                            rsi14 = calc_rsi_wilder(close, 14)
                            rsi14_val = float(rsi14.iloc[-1])
                            if not pd.isna(rsi14_val) and rsi14_val <= D_RSI14_THRESH:
                                tp_price = round(c_last * (1 + D_TAKE_PROFIT), 2)
                                signals_d.append({
                                    'strategy': 'D',
                                    'ticker': tk,
                                    'date': trade_date,
                                    'scan_time': scan_time_kst,
                                    'price': round(c_last, 2),
                                    'rsi14': round(rsi14_val, 1),
                                    'intraday': round(intra * 100, 1),
                                    'ret5d': round(ret5d * 100, 1),
                                    'tp_price': tp_price,
                                    'hold_days': D_MAX_HOLD_DAYS,
                                })
                                print(f"    ★ [D] {tk} @ ${c_last:.2f} | RSI14={rsi14_val:.1f} "
                                      f"Intra={intra*100:.0f}% Ret5d={ret5d*100:.1f}% "
                                      f"TP=${tp_price:.2f}")

                # ══════════════════════════════════════════════
                # Strategy E: $3~$10 + Ret5d<=-25% + Intra>=20% + ConsecDown>=5 + Vol>=20만
                # ══════════════════════════════════════════════
                if run_e and E_PRICE_MIN <= c_last <= E_PRICE_MAX and intra >= E_INTRA_THRESH:
                    if avg_vol >= E_VOL_MIN and consec >= E_CONSEC_DOWN:
                        if n >= 6:
                            ret5d_e = c_last / float(close.iloc[-6]) - 1
                            if ret5d_e <= E_RET5D_THRESH:
                                # D전략과 중복 제외 (pr<=3 & ret5d<=-40 & intra>=30 & rsi14<=25)
                                is_d = False
                                if c_last <= D_PRICE_THRESH and ret5d_e <= D_RET5D_THRESH and intra >= D_INTRA_THRESH:
                                    rsi14_e = calc_rsi_wilder(close, 14)
                                    rsi14_e_val = float(rsi14_e.iloc[-1])
                                    if not pd.isna(rsi14_e_val) and rsi14_e_val <= D_RSI14_THRESH:
                                        is_d = True

                                if not is_d:
                                    tp_price = round(c_last * (1 + E_TAKE_PROFIT), 2)
                                    signals_e.append({
                                        'strategy': 'E',
                                        'ticker': tk,
                                        'date': trade_date,
                                        'scan_time': scan_time_kst,
                                        'price': round(c_last, 2),
                                        'ret5d': round(ret5d_e * 100, 1),
                                        'intraday': round(intra * 100, 1),
                                        'consec_down': consec,
                                        'vol_avg': int(avg_vol),
                                        'tp_price': tp_price,
                                        'hold_days': E_MAX_HOLD_DAYS,
                                    })
                                    print(f"    ★ [E] {tk} @ ${c_last:.2f} | "
                                          f"Ret5d={ret5d_e*100:.1f}% Intra={intra*100:.0f}% "
                                          f"Down={consec}d Vol={avg_vol/1000:.0f}K "
                                          f"TP=${tp_price:.2f}")

                # ══════════════════════════════════════════════
                # Strategy F: Vol20d>10% + 52wHigh<-85% + BB<0 + Ret1d<-5% + GapUp>5%
                # ══════════════════════════════════════════════
                if run_f and n >= 252:
                    # 20일 변동성
                    daily_returns = close.pct_change()
                    vol_20d = float(daily_returns.iloc[-20:].std())

                    if vol_20d > F_VOLATILITY_20D:
                        # 52주 고점 대비 위치
                        high_52w = float(high.rolling(252).max().iloc[-1])
                        if high_52w > 0:
                            dist_52w = (c_last - high_52w) / high_52w

                            if dist_52w < F_DIST_52W_HIGH:
                                # 볼린저 밴드 %B
                                bb_mid = close.rolling(20).mean()
                                bb_std_val = close.rolling(20).std()
                                bb_lower = bb_mid - 2 * bb_std_val
                                bb_upper = bb_mid + 2 * bb_std_val
                                bb_range = float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1])
                                bb_pctb = (c_last - float(bb_lower.iloc[-1])) / bb_range if bb_range > 0 else 0.5

                                if bb_pctb < F_BB_PCTB_THRESH:
                                    # 전일 수익률
                                    if n >= 2:
                                        prev_close = float(close.iloc[-2])
                                        ret_1d = (c_last - prev_close) / prev_close if prev_close > 0 else 0

                                        if ret_1d < F_RET1D_THRESH:
                                            # 갭업 (당일 시가 vs 전일 종가)
                                            gap_pct = (o_last - prev_close) / prev_close if prev_close > 0 else 0

                                            if gap_pct > F_GAP_UP_THRESH:
                                                tp_price = round(c_last * (1 + F_TAKE_PROFIT), 2)
                                                sl_price = round(c_last * (1 + F_STOP_LOSS), 2)
                                                signals_f.append({
                                                    'strategy': 'F',
                                                    'ticker': tk,
                                                    'date': trade_date,
                                                    'scan_time': scan_time_kst,
                                                    'price': round(c_last, 2),
                                                    'vol_20d': round(vol_20d * 100, 1),
                                                    'dist_52w': round(dist_52w * 100, 1),
                                                    'bb_pctb': round(bb_pctb, 3),
                                                    'ret_1d': round(ret_1d * 100, 1),
                                                    'gap_pct': round(gap_pct * 100, 1),
                                                    'tp_price': tp_price,
                                                    'sl_price': sl_price,
                                                    'hold_days': F_MAX_HOLD_DAYS,
                                                })
                                                print(f"    ★ [F] {tk} @ ${c_last:.2f} | "
                                                      f"Vol20d={vol_20d*100:.1f}% 52wH={dist_52w*100:.1f}% "
                                                      f"BB={bb_pctb:.3f} Ret1d={ret_1d*100:.1f}% "
                                                      f"Gap={gap_pct*100:.1f}% "
                                                      f"TP=${tp_price:.2f} SL=${sl_price:.2f}")

                # ══════════════════════════════════════════════
                # Strategy G: Vol20d>10% + RSI7<30 + MACD cross up + GapUp>5% + Price<SMA5
                # ══════════════════════════════════════════════
                if run_g and n >= 30:
                    daily_returns = close.pct_change()
                    vol_20d = float(daily_returns.iloc[-20:].std()) if len(daily_returns) >= 20 else 0

                    if vol_20d > G_VOLATILITY_20D and rsi7_val is not None and rsi7_val < G_RSI7_THRESH:
                        # MACD histogram (12,26,9)
                        ema12 = close.ewm(span=12, adjust=False).mean()
                        ema26 = close.ewm(span=26, adjust=False).mean()
                        macd_line = ema12 - ema26
                        signal_line = macd_line.ewm(span=9, adjust=False).mean()
                        macd_hist = macd_line - signal_line

                        if len(macd_hist) >= 2:
                            hist_today = float(macd_hist.iloc[-1])
                            hist_yesterday = float(macd_hist.iloc[-2])

                            if hist_today > 0 and hist_yesterday < 0:  # cross up
                                # Gap up
                                prev_close = float(close.iloc[-2])
                                gap_pct = (o_last - prev_close) / prev_close if prev_close > 0 else 0

                                if gap_pct > G_GAP_UP_THRESH:
                                    # Price < SMA5
                                    sma5 = float(close.rolling(5).mean().iloc[-1])
                                    if not pd.isna(sma5) and c_last < sma5:
                                        tp_price = round(c_last * (1 + G_TAKE_PROFIT), 2)
                                        sl_price = round(c_last * (1 + G_STOP_LOSS), 2)
                                        signals_g.append({
                                            'strategy': 'G',
                                            'ticker': tk,
                                            'date': trade_date,
                                            'scan_time': scan_time_kst,
                                            'price': round(c_last, 2),
                                            'rsi7': round(rsi7_val, 1),
                                            'vol_20d': round(vol_20d * 100, 1),
                                            'macd_hist': round(hist_today, 4),
                                            'gap_pct': round(gap_pct * 100, 1),
                                            'sma5': round(sma5, 2),
                                            'tp_price': tp_price,
                                            'sl_price': sl_price,
                                            'hold_days': G_MAX_HOLD_DAYS,
                                        })
                                        print(f"    ★ [G] {tk} @ ${c_last:.2f} | "
                                              f"RSI7={rsi7_val:.1f} Vol={vol_20d*100:.1f}% "
                                              f"MACD={hist_today:.4f} Gap={gap_pct*100:.1f}% "
                                              f"SMA5=${sma5:.2f}")

                # ══════════════════════════════════════════════
                # Strategy H: RSI7<30 + 52wHigh<-85% + Ret1d<-5% + GapUp>5% + ATR strongly expanding
                # ══════════════════════════════════════════════
                if run_h and n >= 252 and rsi7_val is not None and rsi7_val < H_RSI7_THRESH:
                    # 52w high distance
                    high_52w = float(high.rolling(252).max().iloc[-1])
                    if high_52w > 0:
                        dist_52w = (c_last - high_52w) / high_52w

                        if dist_52w < H_DIST_52W_HIGH:
                            # Ret 1d
                            prev_close = float(close.iloc[-2]) if n >= 2 else 0
                            ret_1d = (c_last - prev_close) / prev_close if prev_close > 0 else 0

                            if ret_1d < H_RET1D_THRESH:
                                # Gap up
                                gap_pct = (o_last - prev_close) / prev_close if prev_close > 0 else 0

                                if gap_pct > H_GAP_UP_THRESH:
                                    # ATR strongly expanding (5-day ATR change > 25%)
                                    tr = pd.concat([
                                        high - low,
                                        (high - close.shift(1)).abs(),
                                        (low - close.shift(1)).abs()
                                    ], axis=1).max(axis=1)
                                    atr14 = tr.rolling(14).mean()
                                    if len(atr14) >= 6 and not pd.isna(atr14.iloc[-1]) and not pd.isna(atr14.iloc[-6]):
                                        atr_prev = float(atr14.iloc[-6])
                                        atr_now = float(atr14.iloc[-1])
                                        atr_change = (atr_now - atr_prev) / atr_prev if atr_prev > 0 else 0

                                        if atr_change > H_ATR_CHANGE_THRESH:
                                            tp_price = round(c_last * (1 + H_TAKE_PROFIT), 2)
                                            sl_price = round(c_last * (1 + H_STOP_LOSS), 2)
                                            signals_h.append({
                                                'strategy': 'H',
                                                'ticker': tk,
                                                'date': trade_date,
                                                'scan_time': scan_time_kst,
                                                'price': round(c_last, 2),
                                                'rsi7': round(rsi7_val, 1),
                                                'dist_52w': round(dist_52w * 100, 1),
                                                'ret_1d': round(ret_1d * 100, 1),
                                                'gap_pct': round(gap_pct * 100, 1),
                                                'atr_change': round(atr_change * 100, 1),
                                                'tp_price': tp_price,
                                                'sl_price': sl_price,
                                                'hold_days': H_MAX_HOLD_DAYS,
                                            })
                                            print(f"    ★ [H] {tk} @ ${c_last:.2f} | "
                                                  f"RSI7={rsi7_val:.1f} 52wH={dist_52w*100:.1f}% "
                                                  f"Ret1d={ret_1d*100:.1f}% Gap={gap_pct*100:.1f}% "
                                                  f"ATR={atr_change*100:.1f}%")

                # ══════════════════════════════════════════════
                # Strategy I: RSI14<30 + Vol2x + MACD>0 + Ret1d<-5% + Ret5d>0
                # ══════════════════════════════════════════════
                if run_i and n >= 26 and len(vol) >= 26:
                    # RSI14
                    rsi14 = calc_rsi_wilder(close, 14)
                    rsi14_val = float(rsi14.iloc[-1]) if not pd.isna(rsi14.iloc[-1]) else None

                    if rsi14_val is not None and rsi14_val < I_RSI14_THRESH:
                        # Volume ratio: vol / vol_sma_20
                        vol_sma_20 = float(vol.tail(20).mean()) if len(vol) >= 20 else 0
                        vol_ratio = float(vol.iloc[-1]) / max(vol_sma_20, 1.0)

                        if vol_ratio > I_VOL_RATIO_THRESH:
                            # MACD histogram (12,26,9)
                            ema12 = close.ewm(span=12, adjust=False).mean()
                            ema26 = close.ewm(span=26, adjust=False).mean()
                            macd_line = ema12 - ema26
                            signal_line = macd_line.ewm(span=9, adjust=False).mean()
                            macd_hist = macd_line - signal_line

                            hist_today = float(macd_hist.iloc[-1]) if not pd.isna(macd_hist.iloc[-1]) else None

                            if hist_today is not None and hist_today > 0:
                                # return_1d (previous day < -5%)
                                ret_1d = (c_last - float(close.iloc[-2])) / float(close.iloc[-2]) if n >= 2 else 0

                                if ret_1d < I_RET1D_THRESH:
                                    # return_5d (5-day > 0)
                                    ret_5d = c_last / float(close.iloc[-6]) - 1 if n >= 6 else 0

                                    if ret_5d > I_RET5D_THRESH:
                                        tp_price = round(c_last * (1 + I_TAKE_PROFIT), 2)
                                        sl_price = round(c_last * (1 + I_STOP_LOSS), 2)
                                        signals_i.append({
                                            'strategy': 'I',
                                            'ticker': tk,
                                            'date': trade_date,
                                            'scan_time': scan_time_kst,
                                            'price': round(c_last, 2),
                                            'rsi14': round(rsi14_val, 1),
                                            'vol_ratio': round(vol_ratio, 2),
                                            'macd_hist': round(hist_today, 4),
                                            'ret_1d': round(ret_1d * 100, 1),
                                            'ret_5d': round(ret_5d * 100, 1),
                                            'tp_price': tp_price,
                                            'sl_price': sl_price,
                                            'hold_days': I_MAX_HOLD_DAYS,
                                        })
                                        print(f"    ★ [I] {tk} @ ${c_last:.2f} | "
                                              f"RSI14={rsi14_val:.1f} VolRatio={vol_ratio:.2f}x "
                                              f"MACD={hist_today:.4f} Ret1d={ret_1d*100:.1f}% "
                                              f"Ret5d={ret_5d*100:.1f}%")

                # ══════════════════════════════════════════════
                # Strategy J: Vol20d>10% + RSI14<30 + Vol2x + MACD cross up + Stoch<20
                # ══════════════════════════════════════════════
                if run_j and n >= 26 and len(vol) >= 26:
                    # 20-day volatility
                    daily_returns = close.pct_change()
                    vol_20d = float(daily_returns.iloc[-20:].std()) if len(daily_returns) >= 20 else 0

                    if vol_20d > J_VOLATILITY_20D:
                        # RSI14
                        rsi14 = calc_rsi_wilder(close, 14)
                        rsi14_val = float(rsi14.iloc[-1]) if not pd.isna(rsi14.iloc[-1]) else None

                        if rsi14_val is not None and rsi14_val < J_RSI14_THRESH:
                            # Volume ratio: vol / vol_sma_20
                            vol_sma_20 = float(vol.tail(20).mean()) if len(vol) >= 20 else 0
                            vol_ratio = float(vol.iloc[-1]) / max(vol_sma_20, 1.0)

                            if vol_ratio > J_VOL_RATIO_THRESH:
                                # MACD histogram (12,26,9) — check cross up
                                ema12 = close.ewm(span=12, adjust=False).mean()
                                ema26 = close.ewm(span=26, adjust=False).mean()
                                macd_line = ema12 - ema26
                                signal_line = macd_line.ewm(span=9, adjust=False).mean()
                                macd_hist = macd_line - signal_line

                                if len(macd_hist) >= 2:
                                    hist_today = float(macd_hist.iloc[-1])
                                    hist_yesterday = float(macd_hist.iloc[-2])

                                    if hist_yesterday < 0 and hist_today > 0:  # cross up
                                        # Stochastic K (14)
                                        lowest_14 = low.rolling(14).min()
                                        highest_14 = high.rolling(14).max()
                                        stoch_k = 100 * (close - lowest_14) / (highest_14 - lowest_14)
                                        stoch_k_val = float(stoch_k.iloc[-1]) if not pd.isna(stoch_k.iloc[-1]) else None

                                        if stoch_k_val is not None and stoch_k_val < J_STOCH_OVERSOLD:
                                            tp_price = round(c_last * (1 + J_TAKE_PROFIT), 2)
                                            sl_price = round(c_last * (1 + J_STOP_LOSS), 2)
                                            signals_j.append({
                                                'strategy': 'J',
                                                'ticker': tk,
                                                'date': trade_date,
                                                'scan_time': scan_time_kst,
                                                'price': round(c_last, 2),
                                                'vol_20d': round(vol_20d * 100, 1),
                                                'rsi14': round(rsi14_val, 1),
                                                'vol_ratio': round(vol_ratio, 2),
                                                'macd_hist': round(hist_today, 4),
                                                'stoch_k': round(stoch_k_val, 1),
                                                'tp_price': tp_price,
                                                'sl_price': sl_price,
                                                'hold_days': J_MAX_HOLD_DAYS,
                                            })
                                            print(f"    ★ [J] {tk} @ ${c_last:.2f} | "
                                                  f"Vol20d={vol_20d*100:.1f}% RSI14={rsi14_val:.1f} "
                                                  f"VolRatio={vol_ratio:.2f}x MACD={hist_today:.4f} "
                                                  f"Stoch={stoch_k_val:.1f}")

            except Exception as e:
                print(f"  Warning: {tk} phase2 processing failed: {e}")
                continue
        time.sleep(BATCH_DELAY)

    signals_a.sort(key=lambda x: x['rsi7'])
    signals_b.sort(key=lambda x: x['ticker'])
    signals_c.sort(key=lambda x: x['rsi7'])
    signals_d.sort(key=lambda x: x['price'])
    signals_e.sort(key=lambda x: x['ret5d'])
    signals_f.sort(key=lambda x: x['dist_52w'])
    signals_g.sort(key=lambda x: x['rsi7'])
    signals_h.sort(key=lambda x: x['rsi7'])
    signals_i.sort(key=lambda x: x['rsi14'])
    signals_j.sort(key=lambda x: x['rsi14'])

    return signals_a, signals_b, signals_c, signals_d, signals_e, signals_f, signals_g, signals_h, signals_i, signals_j


# ─── Output ───────────────────────────────────────────────────────────────────

def print_results(signals_a, signals_b, signals_c, signals_d, signals_e, signals_f, signals_g, signals_h, signals_i, signals_j):
    """결과 출력"""
    all_sigs = signals_a + signals_b + signals_c + signals_d + signals_e + signals_f + signals_g + signals_h + signals_i + signals_j
    date_str = all_sigs[0]['date'] if all_sigs else datetime.now(KST).strftime('%Y-%m-%d')

    # Strategy A
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY A — Weekly Signal (+5%) — {date_str}")
    print(f"  조건: Intra>20% + Ret3d<-15% + Down>5d + DistLow5<5% + RSI7<20")
    print(f"  청산: +5% 익절 | -20% 손절 | 5일 타임아웃")
    print(f"  백테스트: 승률 90.1% (262건/5년), 최적청산 누적 +515%")
    print(f"{'='*90}")

    if not signals_a:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI7':>6} {'일중%':>7} {'3일%':>7} "
              f"{'연속↓':>5} {'Low5%':>7} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_a:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi7']:>6.1f} "
                  f"{s['intraday']:>6.1f}% {s['ret3d']:>6.1f}% "
                  f"{s['consec_down']:>5}d {s['dist_low5']:>6.1f}% "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_a)} signals")

    # Strategy B
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY B — High-Gain (+15%) — {date_str}")
    print(f"  조건: RSI7<20 + RSI14<35 + ATR>3 + Intra>15% + MA20<=-25% + RevGrowth>0")
    print(f"  청산: +15% 지정가 | -20% 손절 | 10일 종가청산")
    print(f"  백테스트: 승률 90.3% (31건/28적중)")
    print(f"{'='*90}")

    if not signals_b:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI7':>6} {'RSI14':>6} {'ATR배율':>7} {'일중%':>8} "
              f"{'MA20%':>8} {'매출성장':>8} {'매도가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_b:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi7']:>6.1f} {s['rsi14']:>6.1f} "
                  f"{s['atr_ratio']:>7.2f} {s['intra_pct']:>7.1f}% "
                  f"{s['ma20_pos']:>7.1f}% {s['rev_growth']:>7.1f}% "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_b)} signals")

    # Strategy C
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY C — 과매도 반등 (+5%) — {date_str}")
    print(f"  조건: RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + Down>3d + DistLow5<3%")
    print(f"  청산: +5% 익절 | -20% 손절 | 5일 타임아웃")
    print(f"  백테스트: 승률 86.9% (624건/5년), +7% 기준 81.4%")
    print(f"{'='*90}")

    if not signals_c:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI7':>6} {'일중%':>7} {'1일%':>7} "
              f"{'연속↓':>5} {'Low5%':>7} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_c:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi7']:>6.1f} "
                  f"{s['intraday']:>6.1f}% {s['ret1d']:>6.1f}% "
                  f"{s['consec_down']:>5}d {s['dist_low5']:>6.2f}% "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_c)} signals")

    # Strategy D
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY D — 초저가 폭락 반등 (+20%) — {date_str}")
    print(f"  조건: Price<=$3 + Ret5d<=-40% + Intra>=30% + RSI14<=25")
    print(f"  청산: +20% 익절 | 30일 타임아웃")
    print(f"  백테스트: 승률 97.7% (130건/5년), 건당 평균 +18.9%")
    print(f"{'='*90}")

    if not signals_d:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI14':>6} {'일중%':>7} {'5일%':>7} "
              f"{'익절가':>10} {'보유일':>6}")
        print("-" * 90)
        for s in signals_d:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi14']:>6.1f} "
                  f"{s['intraday']:>6.1f}% {s['ret5d']:>6.1f}% "
                  f"${s['tp_price']:>9.2f} {s['hold_days']:>5}d")
        print(f"  Total: {len(signals_d)} signals")

    # Strategy E
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY E — 저가주 급락 반등 (+10%) — {date_str}")
    print(f"  조건: $3~$10 + Ret5d<=-25% + Intra>=20% + ConsecDown>=5 + AvgVol>=20만")
    print(f"  청산: +10% 익절 | 30일 타임아웃")
    print(f"  백테스트: 승률 91.0% (300건/5년), 평균 max +104.9%, 중간 도달일 2일")
    print(f"{'='*90}")

    if not signals_e:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'5일%':>7} {'일중%':>7} "
              f"{'연속↓':>5} {'거래량':>10} {'익절가':>10} {'보유일':>6}")
        print("-" * 90)
        for s in signals_e:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} "
                  f"{s['ret5d']:>6.1f}% {s['intraday']:>6.1f}% "
                  f"{s['consec_down']:>5}d {s['vol_avg']:>10,} "
                  f"${s['tp_price']:>9.2f} {s['hold_days']:>5}d")
        print(f"  Total: {len(signals_e)} signals")

    # Strategy F
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY F — 바닥 급등 (+50%) — {date_str}")
    print(f"  조건: Vol20d>10% + 52wHigh<-85% + BB<0 + Ret1d<-5% + GapUp>5%")
    print(f"  청산: +50% 익절 | -20% 손절 | 20일 타임아웃")
    print(f"  백테스트: 승률 87.5% (8건/5년), 20일내 50%+ 급등")
    print(f"{'='*90}")

    if not signals_f:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'Vol20d':>7} {'52wH%':>7} {'BB%B':>7} "
              f"{'1일%':>7} {'Gap%':>7} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_f:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} "
                  f"{s['vol_20d']:>6.1f}% {s['dist_52w']:>6.1f}% "
                  f"{s['bb_pctb']:>7.3f} {s['ret_1d']:>6.1f}% "
                  f"{s['gap_pct']:>6.1f}% "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_f)} signals")

    # Strategy G
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY G — MACD 전환 급등 (+40%) — {date_str}")
    print(f"  조건: Vol20d>10% + RSI7<30 + MACD 골든크로스 + GapUp>5% + Price<SMA5")
    print(f"  청산: +40% 익절 | -20% 손절 | 20일 타임아웃")
    print(f"  백테스트: 승률 90.0% (10건/5년), 20일내 40%+ 급등")
    print(f"{'='*90}")
    if not signals_g:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI7':>6} {'Vol20d':>7} {'MACD':>8} "
              f"{'Gap%':>7} {'SMA5':>8} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_g:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi7']:>6.1f} "
                  f"{s['vol_20d']:>6.1f}% {s['macd_hist']:>8.4f} "
                  f"{s['gap_pct']:>6.1f}% ${s['sma5']:>7.2f} "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_g)} signals")

    # Strategy H
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY H — ATR 확대 급등 (+40%) — {date_str}")
    print(f"  조건: RSI7<30 + 52wHigh<-85% + Ret1d<-5% + GapUp>5% + ATR확대>25%")
    print(f"  청산: +40% 익절 | -20% 손절 | 20일 타임아웃")
    print(f"  백테스트: 승률 90.0% (10건/5년), 20일내 40%+ 급등")
    print(f"{'='*90}")
    if not signals_h:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI7':>6} {'52wH%':>7} {'1일%':>7} "
              f"{'Gap%':>7} {'ATR%':>7} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_h:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi7']:>6.1f} "
                  f"{s['dist_52w']:>6.1f}% {s['ret_1d']:>6.1f}% "
                  f"{s['gap_pct']:>6.1f}% {s['atr_change']:>6.1f}% "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_h)} signals")

    # Strategy I
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY I — 과매도 반등 단타 (+10%) — {date_str}")
    print(f"  조건: RSI14<30 + Vol2x + MACD>0 + Ret1d<-5% + Ret5d>0")
    print(f"  청산: +10% 익절 | -20% 손절 | 5일 타임아웃")
    print(f"  백테스트: 승률 100% (8건/5년)")
    print(f"{'='*90}")
    if not signals_i:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'RSI14':>6} {'Vol배율':>7} {'MACD':>8} "
              f"{'1일%':>7} {'5일%':>7} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_i:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['rsi14']:>6.1f} "
                  f"{s['vol_ratio']:>7.2f}x {s['macd_hist']:>8.4f} "
                  f"{s['ret_1d']:>6.1f}% {s['ret_5d']:>6.1f}% "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_i)} signals")

    # Strategy J
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY J — MACD 전환 단타 (+10%) — {date_str}")
    print(f"  조건: Vol20d>10% + RSI14<30 + Vol2x + MACD cross up + Stoch<20")
    print(f"  청산: +10% 익절 | -20% 손절 | 5일 타임아웃")
    print(f"  백테스트: 승률 90% (9/10, 5년)")
    print(f"{'='*90}")
    if not signals_j:
        print("  신호 없음")
    else:
        print(f"{'Ticker':<8} {'종가':>8} {'Vol20d':>7} {'RSI14':>6} {'Vol배율':>7} "
              f"{'MACD':>8} {'Stoch':>7} {'익절가':>10} {'손절가':>10}")
        print("-" * 90)
        for s in signals_j:
            print(f"{s['ticker']:<8} {s['price']:>8.2f} {s['vol_20d']:>6.1f}% "
                  f"{s['rsi14']:>6.1f} {s['vol_ratio']:>7.2f}x "
                  f"{s['macd_hist']:>8.4f} {s['stoch_k']:>6.1f} "
                  f"${s['tp_price']:>9.2f} ${s['sl_price']:>9.2f}")
        print(f"  Total: {len(signals_j)} signals")


def save_results(signals_a, signals_b, signals_c, signals_d, signals_e, signals_f, signals_g, signals_h, signals_i, signals_j):
    """CSV 저장: data/signal_YYYY-MM-DD.csv + data/history.csv"""
    os.makedirs('data', exist_ok=True)
    all_signals = signals_a + signals_b + signals_c + signals_d + signals_e + signals_f + signals_g + signals_h + signals_i + signals_j

    # 실제 거래일 기준 파일명 (신호가 있으면 신호의 date, 없으면 KST 날짜)
    if all_signals:
        date_str = all_signals[0]['date']
    else:
        date_str = datetime.now(KST).strftime('%Y-%m-%d')

    # 오늘자 신호 파일
    daily_path = f"data/signal_{date_str}.csv"
    if all_signals:
        df = pd.DataFrame(all_signals)
        df.to_csv(daily_path, index=False)
        print(f"\n  저장: {daily_path} ({len(all_signals)}건)")
    else:
        # 빈 파일도 생성 (GitHub Actions artifact용)
        pd.DataFrame(columns=['strategy','ticker','date','price']).to_csv(daily_path, index=False)
        print(f"\n  저장: {daily_path} (신호 없음)")

    # 히스토리 누적
    hist_path = "data/history.csv"
    if all_signals:
        df_new = pd.DataFrame(all_signals)
        if os.path.exists(hist_path):
            df_hist = pd.read_csv(hist_path)
            df_hist = pd.concat([df_hist, df_new], ignore_index=True)
            df_hist.drop_duplicates(subset=['strategy','ticker','date'], keep='last', inplace=True)
        else:
            df_hist = df_new
        df_hist.to_csv(hist_path, index=False)
        print(f"  히스토리: {hist_path} ({len(df_hist)}건 누적)")

    # ── Data Quality Validation ──
    dq_warnings = []
    for sig in all_signals:
        tk = sig.get('ticker', '?')
        strat = sig.get('strategy', '?')
        price = sig.get('price', 0)
        # Price sanity check
        if price <= 0:
            dq_warnings.append(f"  ⚠ DQ: [{strat}] {tk} — price={price} (zero or negative)")
        # Volume check (only E has vol_avg)
        vol = sig.get('vol_avg', None)
        if vol is not None and vol < 50000:
            dq_warnings.append(f"  ⚠ DQ: [{strat}] {tk} — low volume={vol:,} (<50K)")
        # TP price sanity
        tp = sig.get('tp_price', 0)
        if tp > 0 and price > 0 and tp < price:
            dq_warnings.append(f"  ⚠ DQ: [{strat}] {tk} — TP=${tp} < entry=${price}")
        # RSI range check
        for rsi_key in ['rsi7', 'rsi14']:
            rsi_v = sig.get(rsi_key, None)
            if rsi_v is not None and (rsi_v < 0 or rsi_v > 100):
                dq_warnings.append(f"  ⚠ DQ: [{strat}] {tk} — {rsi_key}={rsi_v} (out of range)")

    if dq_warnings:
        print(f"\n{'='*60}")
        print(f"  DATA QUALITY WARNINGS ({len(dq_warnings)} issues)")
        print(f"{'='*60}")
        for w in dq_warnings:
            print(w)
    else:
        print("\n  ✓ Data quality check passed — no issues found")

    # JSON summary (Streamlit용)
    summary = {
        'scan_date': date_str,
        'scan_time': datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST'),
        'strategy_a_count': len(signals_a),
        'strategy_b_count': len(signals_b),
        'strategy_c_count': len(signals_c),
        'strategy_d_count': len(signals_d),
        'strategy_e_count': len(signals_e),
        'strategy_f_count': len(signals_f),
        'strategy_g_count': len(signals_g),
        'strategy_h_count': len(signals_h),
        'strategy_i_count': len(signals_i),
        'strategy_j_count': len(signals_j),
        'total_count': len(all_signals),
        'dq_warnings': len(dq_warnings),
    }
    with open('data/latest_scan.json', 'w') as f:
        json.dump(summary, f, indent=2)

    return daily_path


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='US Stock Surge Scanner (A+B+C+D+E+F+G+H+I+J)')
    parser.add_argument('--strategy', default='ABCDEFGHIJ',
                       help='Which strategies to run, e.g. A, AB, ABCDEFGHIJ (default: ABCDEFGHIJ)')
    args = parser.parse_args()

    t0 = time.time()
    date_str = datetime.now(KST).strftime('%Y-%m-%d %H:%M')
    strat_str = args.strategy

    print("=" * 80)
    print("  US Stock Surge Detection — Combined Scanner")
    print(f"  Strategy: {strat_str}")
    print(f"  Scan Time: {date_str}")
    print("=" * 80)

    # [0] yfinance 데이터 갱신 대기
    confirmed_date = wait_for_market_data(max_retries=3, retry_interval=120)
    if confirmed_date:
        print(f"\n  확인된 거래일: {confirmed_date}")

    # [1] 종목 수집
    print("\n[1] 종목 수집 중...")
    all_tickers = get_all_tickers()

    # [2] Phase 1: 공통 필터 (한 번만 스캔)
    print("\n[2] Phase 1: 공통 필터...")
    candidates = phase1_rsi_filter(all_tickers, strat_str)

    # [3] Phase 2: 전략별 정밀 분석 (한 번의 다운로드)
    print("\n[3] Phase 2: 전략별 정밀 분석...")
    signals_a, signals_b, signals_c, signals_d, signals_e, signals_f, signals_g, signals_h, signals_i, signals_j = phase2_check_all(candidates, strat_str)

    # [4] 결과 출력 및 저장
    print_results(signals_a, signals_b, signals_c, signals_d, signals_e, signals_f, signals_g, signals_h, signals_i, signals_j)
    save_results(signals_a, signals_b, signals_c, signals_d, signals_e, signals_f, signals_g, signals_h, signals_i, signals_j)

    elapsed = time.time() - t0
    print(f"\n  스캔 완료: {elapsed:.0f}초")
    print(f"  Strategy A: {len(signals_a)}건 | B: {len(signals_b)}건 | C: {len(signals_c)}건 | D: {len(signals_d)}건 | E: {len(signals_e)}건 | F: {len(signals_f)}건 | G: {len(signals_g)}건 | H: {len(signals_h)}건 | I: {len(signals_i)}건 | J: {len(signals_j)}건")


if __name__ == '__main__':
    main()
