#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Detection — Combined Scanner (Strategy A + B)

  Strategy A (+5% in 5 Days):
    매수: Intra>20% + Ret3d<-15% + ConsecDown>5 + DistLow5<5% + RSI7<20
    매도: +5% 익절 | -20% 손절 | -3% 트레일링 | 5일 타임아웃
    백테스트: 90.1% (236/262), 최적청산 누적 +515%

  Strategy B (+15% High-Gain):
    매수: RSI7<20 + RSI14<35 + ATR>3 + Intra>15% + MA20<=-25% + RevGrowth>0
    매도: +15% 지정가 | -20% 손절 | 10일 종가청산
    백테스트: 90.3% (28/31)

  Usage:
    python scanner.py                    # Scan both strategies
    python scanner.py --strategy A       # Strategy A only
    python scanner.py --strategy B       # Strategy B only

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


# ─── Strategy A Scanner (+5%) ────────────────────────────────────────────────

def scan_strategy_a(all_tickers):
    """Strategy A: +5% in 5 Days (승률 90.1%, 262건/5년)
    조건: Intra>20% + Ret3d<-15% + ConsecDown>5 + DistLow5<5% + RSI7<20
    """
    print(f"\n{'='*80}")
    print("  [Strategy A] +5% in 5 Days Scanner")
    print(f"{'='*80}")

    # Phase 1: 빠른 필터링 (RSI < 30으로 넓게)
    print(f"\n  Phase 1: 빠른 필터링 ({len(all_tickers)}개)...")
    candidates = []
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
                # ★ 20일 평균거래량 필터 (phase3 동일)
                avg_vol = float(df['Volume'].dropna().tail(20).mean())
                if last_close < MIN_PRICE or avg_vol < MIN_VOLUME:
                    continue
                # 넓은 RSI 필터 (Phase 2에서 정밀 체크)
                rsi7 = calc_rsi_wilder(close, 7)
                if not pd.isna(rsi7.iloc[-1]) and float(rsi7.iloc[-1]) < 30:
                    candidates.append(tk)
            except:
                continue
        time.sleep(BATCH_DELAY)

    print(f"  Phase 1 완료: {len(candidates)}개 후보")

    # Phase 2: 5개 조건 정밀 체크
    print(f"\n  Phase 2: 5개 조건 정밀 분석 ({len(candidates)}개)...")
    signals = []

    for b_idx in range(0, len(candidates), 20):
        batch = candidates[b_idx:b_idx + 20]
        data = download_batch(batch, period='90d')
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

                if o_last <= 0:
                    continue

                # ★ 20일 평균거래량 필터 (phase3 동일)
                avg_vol = float(vol.tail(20).mean())
                if c_last < MIN_PRICE or avg_vol < MIN_VOLUME:
                    continue

                # ── 조건 1: 일중 변동폭 > 20% ──
                intra = (h_last - l_last) / o_last
                if intra <= A_INTRA_THRESH:
                    continue

                # ── 조건 2: 3일 수익률 < -15% ──
                if n < 4:
                    continue
                ret3d = c_last / float(close.iloc[-4]) - 1
                if ret3d >= A_RET3D_THRESH:
                    continue

                # ── 조건 3: 연속 하락일 > 5 ──
                consec = calc_consec_down(close)
                if consec <= A_CONSEC_DOWN:
                    continue

                # ── 조건 4: 5일 최저가 대비 < 5% ──
                if n < 5:
                    continue
                low5_min = float(low.iloc[-5:].min())
                # ★ 분모 = low5_min (phase3 동일)
                dist_low5 = (c_last - low5_min) / max(low5_min, 0.01)
                if dist_low5 >= A_DIST_LOW5:
                    continue

                # ── 조건 5: RSI(7) < 20 ──
                rsi7 = calc_rsi_wilder(close, 7)
                rsi7_val = float(rsi7.iloc[-1])
                if pd.isna(rsi7_val) or rsi7_val >= A_RSI7_THRESH:
                    continue

                # ★ 5개 조건 모두 충족 ★
                tp_price = round(c_last * (1 + A_TAKE_PROFIT), 2)
                sl_price = round(c_last * (1 + A_STOP_LOSS), 2)

                signals.append({
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
                print(f"    ★ SIGNAL: {tk} @ ${c_last:.2f} | RSI7={rsi7_val:.1f} "
                      f"Intra={intra*100:.0f}% Ret3d={ret3d*100:.1f}% "
                      f"Down={consec}d Dist={dist_low5*100:.1f}%")

            except:
                continue
        time.sleep(BATCH_DELAY)

    signals.sort(key=lambda x: x['rsi7'])
    return signals


# ─── Strategy B Scanner (+15%) ───────────────────────────────────────────────

def scan_strategy_b(all_tickers):
    """Strategy B: +15% High-Gain (승률 90.3%, 31건)
    조건: RSI7<20 + RSI14<35 + ATR>3 + Intra>15% + MA20<=-25% + RevGrowth>0
    """
    print(f"\n{'='*80}")
    print("  [Strategy B] +15% High-Gain Scanner")
    print(f"{'='*80}")

    # Phase 1: 빠른 필터링
    print(f"\n  Phase 1: 빠른 필터링 ({len(all_tickers)}개)...")
    candidates = []
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
                # ★ 20일 평균거래량 필터
                avg_vol = float(df['Volume'].dropna().tail(20).mean())
                if last_close < MIN_PRICE or avg_vol < MIN_VOLUME:
                    continue
                rsi7 = calc_rsi_wilder(close, 7)
                if not pd.isna(rsi7.iloc[-1]) and float(rsi7.iloc[-1]) < 30:
                    candidates.append(tk)
            except:
                continue
        time.sleep(BATCH_DELAY)

    print(f"  Phase 1 완료: {len(candidates)}개 후보")

    # Phase 2: 6개 조건 정밀 체크
    print(f"\n  Phase 2: 6개 조건 정밀 분석 ({len(candidates)}개)...")
    signals = []

    for b_idx in range(0, len(candidates), 20):
        batch = candidates[b_idx:b_idx + 20]
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

                if c_last < MIN_PRICE or o_last <= 0:
                    continue

                # ★ 20일 평균거래량 필터
                avg_vol = float(vol.tail(20).mean())
                if avg_vol < MIN_VOLUME:
                    continue

                # === 조건 1: RSI7 < 20 ===
                rsi7 = calc_rsi_wilder(close, 7)
                rsi7_val = float(rsi7.iloc[-1])
                if pd.isna(rsi7_val) or rsi7_val >= B_RSI7_THRESH:
                    continue

                # === 조건 2: ATR ratio > 3 (SMA(TR,5)/SMA(TR,20)) ===
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
                if pd.isna(atr_ratio) or atr_ratio <= B_ATR_RATIO_THRESH:
                    continue

                # === 조건 3: revenueGrowth > 0 (yfinance info에서 가져오기) ===
                try:
                    info = yf.Ticker(tk).info
                    rev_growth = info.get('revenueGrowth', None)
                    if rev_growth is None or rev_growth <= 0:
                        continue
                except:
                    continue

                # === 조건 4: MA20 position <= -25% ===
                ma20 = close.rolling(20).mean()
                ma20_val = float(ma20.iloc[-1])
                if pd.isna(ma20_val) or ma20_val <= 0:
                    continue
                ma20_pos = (c_last - ma20_val) / ma20_val
                if ma20_pos > B_MA20_THRESH:
                    continue

                # === 조건 5: RSI14 < 35 ===
                rsi14 = calc_rsi_wilder(close, 14)
                rsi14_val = float(rsi14.iloc[-1])
                if pd.isna(rsi14_val) or rsi14_val >= B_RSI14_THRESH:
                    continue

                # === 조건 6: Intraday range > 15% ===
                intra = (h_last - l_last) / o_last
                if intra <= B_INTRA_THRESH:
                    continue

                # ★ 6개 조건 모두 충족 ★
                tp_price = round(c_last * (1 + B_TAKE_PROFIT), 2)
                sl_price = round(c_last * (1 + B_STOP_LOSS), 2)

                signals.append({
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
                print(f"    ★ SIGNAL: {tk} @ ${c_last:.2f} | RSI7={rsi7_val:.1f} "
                      f"RSI14={rsi14_val:.1f} ATR={atr_ratio:.1f} "
                      f"Intra={intra*100:.0f}% MA20={ma20_pos*100:.1f}%")

            except:
                continue
        time.sleep(BATCH_DELAY)

    signals.sort(key=lambda x: x['ticker'])
    return signals


# ─── Output ───────────────────────────────────────────────────────────────────

def print_results(signals_a, signals_b):
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


def save_results(signals_a, signals_b):
    """CSV 저장: data/signal_YYYY-MM-DD.csv + data/history.csv"""
    os.makedirs('data', exist_ok=True)
    date_str = datetime.now().strftime('%Y-%m-%d')
    all_signals = signals_a + signals_b

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
        'total_count': len(all_signals),
    }
    with open('data/latest_scan.json', 'w') as f:
        json.dump(summary, f, indent=2)

    return daily_path


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='US Stock Surge Scanner (A+B)')
    parser.add_argument('--strategy', choices=['A', 'B', 'AB'], default='AB',
                       help='Which strategy to run (default: AB = both)')
    args = parser.parse_args()

    t0 = time.time()
    date_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    print("=" * 80)
    print("  US Stock Surge Detection — Combined Scanner")
    print(f"  Strategy A (+5%) + Strategy B (+15%)")
    print(f"  Scan Time: {date_str}")
    print("=" * 80)

    # 종목 수집
    print("\n[1] 종목 수집 중...")
    all_tickers = get_all_tickers()

    signals_a = []
    signals_b = []

    # Strategy A
    if 'A' in args.strategy:
        print(f"\n[2] Strategy A 스캔 중...")
        signals_a = scan_strategy_a(all_tickers)

    # Strategy B
    if 'B' in args.strategy:
        print(f"\n[3] Strategy B 스캔 중...")
        signals_b = scan_strategy_b(all_tickers)

    # 결과 출력 및 저장
    print_results(signals_a, signals_b)
    save_results(signals_a, signals_b)

    elapsed = time.time() - t0
    print(f"\n  스캔 완료: {elapsed:.0f}초")
    print(f"  Strategy A: {len(signals_a)}건 | Strategy B: {len(signals_b)}건")


if __name__ == '__main__':
    main()
