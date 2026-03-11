#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Detection — Combined Scanner (Strategy A + B + C + D + E)

  Strategy A (+5% in 5 Days):
    매수: Intra>20% + Ret3d<-15% + ConsecDown>5 + DistLow5<5% + RSI7<20
    매도: +5% 익절 | -20% 손절 | -3% 트레일링 | 5일 타임아웃
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

  Usage:
    python scanner.py                      # Scan all strategies (A+B+C+D+E)
    python scanner.py --strategy A         # Strategy A only
    python scanner.py --strategy DE        # Strategy D + E only
    python scanner.py --strategy ABCDE     # All strategies

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
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ─── Configuration ────────────────────────────────────────────────────────────

# === Strategy A thresholds (+5%) ===
A_INTRA_THRESH   = 0.20     # 일중 변동폭 > 20%
A_RET3D_THRESH   = -0.15    # 3일 수익률 < -15%
A_CONSEC_DOWN    = 5         # 연속 하락 > 5일
A_DIST_LOW5      = 0.05      # 5일 최저가 대비 < 5%
A_RSI7_THRESH    = 20        # RSI(7) < 20
A_TAKE_PROFIT    = 0.05      # +5%
A_STOP_LOSS      = -0.20     # -20%
A_TRAILING_STOP  = -0.03     # -3% 트레일링
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

# === Common filters ===
MIN_PRICE      = 1.0
MIN_VOLUME     = 10000       # 20일 평균거래량 기준
BATCH_SIZE     = 80
BATCH_DELAY    = 1.5

# === Exclude leveraged ETFs ===
LEVERAGED_ETF = {
    'TQQQ','SQQQ','UPRO','SPXU','UDOW','SDOW','QLD','QID','SSO','SDS',
    'LABU','LABD','NUGT','DUST','JNUG','JDST','FNGU','FNGD','SOXL','SOXS',
    'TNA','TZA','SPXS','SPXL','TECL','TECS','FAS','FAZ','ERX','ERY',
    'CURE','UVXY','SVXY','VXX','VIXY','TVIX','UCO','SCO','BOIL','KOLD',
    'UNG','DGAZ','UGAZ','AGQ','ZSL','USLV','DSLV','GDXU','GDXD',
    'YANG','YINN','CWEB','EDC','EDZ','MEXX','RETL','DRIP','GUSH',
    'NAIL','DRV','DPST','BNKU','WEBL','WEBS','MSTZ','MSTU',
    'TSLL','TSDD','NVDL','NVDS','CONL','CONY','BITX','BITU',
}

# ─── Helper Functions ─────────────────────────────────────────────────────────

def calc_rsi_wilder(close_series, period=7):
    """Wilder RSI (SMA seed + Wilder smoothing) — 백테스트와 동일"""
    delta = close_series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    for i in range(period, len(close_series)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period-1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period-1) + loss.iloc[i]) / period
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))


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


def extract_ticker_df(data, tk, batch_size):
    """yfinance 반환값에서 특정 티커의 DataFrame을 안전하게 추출.
    yfinance 2.51+ 에서는 단일/복수 티커 모두 MultiIndex 컬럼을 반환할 수 있음.
    """
    if data is None or data.empty:
        return None

    cols = data.columns

    # Case 1: 단일 인덱스 (단일 티커이거나 구버전 yfinance)
    if not isinstance(cols, pd.MultiIndex):
        if batch_size == 1:
            return data.dropna(how='all')
        return None

    # Case 2: MultiIndex 컬럼
    level_values = [set(cols.get_level_values(i)) for i in range(cols.nlevels)]

    # 티커가 어느 레벨에 있는지 탐지
    ticker_level = None
    for i, vals in enumerate(level_values):
        if tk in vals:
            ticker_level = i
            break

    if ticker_level is None:
        return None

    try:
        df = data.xs(tk, level=ticker_level, axis=1)
        df = df.dropna(how='all')
        return df if len(df) > 0 else None
    except (KeyError, TypeError):
        return None


def get_all_tickers():
    """NASDAQ/NYSE/AMEX 전체 상장 종목 수집"""
    tickers = set()
    urls = [
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                for line in resp.text.strip().split('\n')[1:]:
                    parts = line.split('|')
                    if len(parts) >= 2:
                        sym = parts[1].strip() if 'nasdaqtraded' in url else parts[0].strip()
                        if sym and sym.isalpha() and 1 <= len(sym) <= 5 and sym != 'Symbol':
                            tickers.add(sym)
                print(f"  [{url.split('/')[-1]}] {len(tickers)} tickers")
        except Exception as e:
            print(f"  Warning: {url.split('/')[-1]}: {e}")

    if len(tickers) < 1000:
        try:
            resp = requests.get("https://www.sec.gov/files/company_tickers.json",
                              headers={"User-Agent": "SurgeScanner/2.0 scanner@example.com"}, timeout=15)
            if resp.status_code == 200:
                for item in resp.json().values():
                    sym = item.get('ticker', '').strip()
                    if sym and 1 <= len(sym) <= 5 and sym.isalpha():
                        tickers.add(sym)
        except:
            pass

    tickers -= LEVERAGED_ETF
    tickers = {t for t in tickers if not t.endswith('W') and not any(c.isdigit() for c in t)}
    print(f"  Total: {len(tickers)} tickers")
    return sorted(tickers)


def download_batch(tickers, period='60d'):
    """yfinance 배치 다운로드 (재시도 포함)"""
    for attempt in range(3):
        try:
            data = yf.download(' '.join(tickers), period=period,
                             group_by='ticker', progress=False, threads=True, timeout=30)
            return data
        except:
            if attempt < 2: time.sleep(3)
    return None


# ─── Unified Phase 1: 공통 RSI 필터 ─────────────────────────────────────────

def phase1_rsi_filter(all_tickers, strat_str):
    """Phase 1: 공통 필터링 (A/B/C: RSI7<35, D: Price<=$3, E: Price $3~$10)
    한 번의 30d 스캔으로 모든 전략 후보를 수집.
    """
    run_d = 'D' in strat_str
    run_e = 'E' in strat_str
    run_abc = any(s in strat_str for s in ['A', 'B', 'C'])

    print(f"\n{'='*80}")
    print(f"  [Phase 1] 공통 필터링 (RSI7<35{' + Price<=$3' if run_d else ''}{' + Price $3~$10' if run_e else ''})")
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
            except:
                continue
        time.sleep(BATCH_DELAY)

    candidates = sorted(candidates)
    print(f"  Phase 1 완료: {len(candidates)}개 후보")
    return candidates


# ─── Unified Phase 2: 한 번의 다운로드로 A/B/C 동시 체크 ────────────────────

def phase2_check_all(candidates, strat_str):
    """Phase 2: 120d 데이터를 한 번만 받아서 A/B/C/D 조건을 모두 체크"""
    print(f"\n{'='*80}")
    print(f"  [Phase 2] 정밀 분석 ({len(candidates)}개 후보)")
    print(f"{'='*80}")

    signals_a = []
    signals_b = []
    signals_c = []
    signals_d = []
    signals_e = []

    run_a = 'A' in strat_str
    run_b = 'B' in strat_str
    run_c = 'C' in strat_str
    run_d = 'D' in strat_str
    run_e = 'E' in strat_str

    for b_idx in range(0, len(candidates), 20):
        batch = candidates[b_idx:b_idx + 20]
        batch_num = b_idx // 20 + 1
        total_p2 = (len(candidates) + 19) // 20

        if batch_num % 10 == 1 or batch_num == total_p2:
            print(f"    Phase2 Batch {batch_num}/{total_p2} | "
                  f"A:{len(signals_a)} B:{len(signals_b)} C:{len(signals_c)} D:{len(signals_d)} E:{len(signals_e)}")

        # 120d로 한 번만 다운로드 (B가 MA20에 120d 필요)
        data = download_batch(batch, period='120d')
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

                if o_last <= 0 or c_last < MIN_PRICE:
                    continue

                avg_vol = float(vol.tail(20).mean())
                if avg_vol < MIN_VOLUME:
                    continue

                # ── 공통 계산 (한 번만) ──
                rsi7 = calc_rsi_wilder(close, 7)
                rsi7_val = float(rsi7.iloc[-1])
                if pd.isna(rsi7_val):
                    continue

                intra = (h_last - l_last) / o_last
                consec = calc_consec_down(close)

                low5_min = float(low.iloc[-5:].min()) if n >= 5 else None
                dist_low5 = (c_last - low5_min) / max(low5_min, 0.01) if low5_min else None

                # ══════════════════════════════════════════════
                # Strategy A: Intra>20% + Ret3d<-15% + Down>5 + DistLow5<5% + RSI7<20
                # ══════════════════════════════════════════════
                if run_a and rsi7_val < A_RSI7_THRESH:
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
                                'date': datetime.now().strftime('%Y-%m-%d'),
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
                if run_b and rsi7_val < B_RSI7_THRESH and intra > B_INTRA_THRESH:
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
                                        rev_growth = info.get('revenueGrowth', None)
                                        if rev_growth is not None and rev_growth > 0:
                                            tp_price = round(c_last * (1 + B_TAKE_PROFIT), 2)
                                            sl_price = round(c_last * (1 + B_STOP_LOSS), 2)
                                            signals_b.append({
                                                'strategy': 'B',
                                                'ticker': tk,
                                                'date': datetime.now().strftime('%Y-%m-%d'),
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
                                    except:
                                        pass

                # ══════════════════════════════════════════════
                # Strategy C: RSI7<30 + Intra>20% + Ret1d<-8% + 전일하락 + Down>3 + DistLow5<3%
                # ══════════════════════════════════════════════
                if run_c and rsi7_val < C_RSI7_THRESH and intra > C_INTRA_THRESH:
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
                                'date': datetime.now().strftime('%Y-%m-%d'),
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
                                    'date': datetime.now().strftime('%Y-%m-%d'),
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
                                        'date': datetime.now().strftime('%Y-%m-%d'),
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

            except:
                continue
        time.sleep(BATCH_DELAY)

    signals_a.sort(key=lambda x: x['rsi7'])
    signals_b.sort(key=lambda x: x['ticker'])
    signals_c.sort(key=lambda x: x['rsi7'])
    signals_d.sort(key=lambda x: x['price'])
    signals_e.sort(key=lambda x: x['ret5d'])

    return signals_a, signals_b, signals_c, signals_d, signals_e


# ─── Output ───────────────────────────────────────────────────────────────────

def print_results(signals_a, signals_b, signals_c, signals_d, signals_e):
    """결과 출력"""
    date_str = datetime.now().strftime('%Y-%m-%d')

    # Strategy A
    print(f"\n{'='*90}")
    print(f"  ★ STRATEGY A — Weekly Signal (+5%) — {date_str}")
    print(f"  조건: Intra>20% + Ret3d<-15% + Down>5d + DistLow5<5% + RSI7<20")
    print(f"  청산: +5% 익절 | -20% 손절 | -3% 트레일링 | 5일 타임아웃")
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


def save_results(signals_a, signals_b, signals_c, signals_d, signals_e):
    """CSV 저장: data/signal_YYYY-MM-DD.csv + data/history.csv"""
    os.makedirs('data', exist_ok=True)
    date_str = datetime.now().strftime('%Y-%m-%d')
    all_signals = signals_a + signals_b + signals_c + signals_d + signals_e

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

    # JSON summary (Streamlit용)
    summary = {
        'scan_date': date_str,
        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'strategy_a_count': len(signals_a),
        'strategy_b_count': len(signals_b),
        'strategy_c_count': len(signals_c),
        'strategy_d_count': len(signals_d),
        'strategy_e_count': len(signals_e),
        'total_count': len(all_signals),
    }
    with open('data/latest_scan.json', 'w') as f:
        json.dump(summary, f, indent=2)

    return daily_path


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='US Stock Surge Scanner (A+B+C+D+E)')
    parser.add_argument('--strategy', default='ABCDE',
                       help='Which strategies to run, e.g. A, AB, ABCDE (default: ABCDE)')
    args = parser.parse_args()

    t0 = time.time()
    date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    strat_str = args.strategy

    print("=" * 80)
    print("  US Stock Surge Detection — Combined Scanner")
    print(f"  Strategy: {strat_str}")
    print(f"  Scan Time: {date_str}")
    print("=" * 80)

    # [1] 종목 수집
    print("\n[1] 종목 수집 중...")
    all_tickers = get_all_tickers()

    # [2] Phase 1: 공통 필터 (한 번만 스캔)
    print("\n[2] Phase 1: 공통 필터...")
    candidates = phase1_rsi_filter(all_tickers, strat_str)

    # [3] Phase 2: 전략별 정밀 분석 (한 번의 다운로드)
    print("\n[3] Phase 2: 전략별 정밀 분석...")
    signals_a, signals_b, signals_c, signals_d, signals_e = phase2_check_all(candidates, strat_str)

    # [4] 결과 출력 및 저장
    print_results(signals_a, signals_b, signals_c, signals_d, signals_e)
    save_results(signals_a, signals_b, signals_c, signals_d, signals_e)

    elapsed = time.time() - t0
    print(f"\n  스캔 완료: {elapsed:.0f}초")
    print(f"  Strategy A: {len(signals_a)}건 | B: {len(signals_b)}건 | C: {len(signals_c)}건 | D: {len(signals_d)}건 | E: {len(signals_e)}건")


if __name__ == '__main__':
    main()
