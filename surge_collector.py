"""
surge_collector.py
==================
하루 만에 전일 종가 대비 200%+ 급등한 종목을 5년간 수집하고,
각 케이스에 대해 50개+ 지표를 계산하여 공통점을 분석한다.

Output:
  data/surge_200pct_events.csv   — 전체 이벤트 + 50개 지표
  data/surge_200pct_summary.json — 연도별 통계 + 공통점 분석
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import logging
import warnings
from datetime import datetime, timedelta
from collections import Counter

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────
START_DATE = "2020-04-01"
END_DATE   = "2025-04-01"
SURGE_THRESHOLD = 200  # 전일종가 대비 %
MIN_HISTORY_DAYS = 60  # 지표 계산에 필요한 최소 이력
BATCH_SIZE = 80
OUTPUT_DIR = "data"

# ── 헬퍼: RSI (Wilder) ────────────────────────────────
def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)

# ── 헬퍼: Bollinger Bands ─────────────────────────────
def calc_bollinger(close, period=20, num_std=2):
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = ma + num_std * std
    lower = ma - num_std * std
    width = (upper - lower) / ma * 100
    pos = (close - lower) / (upper - lower)
    return ma, upper, lower, width, pos

# ── 헬퍼: MACD ────────────────────────────────────────
def calc_macd(close):
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    hist = macd - signal
    return macd, signal, hist

# ── 헬퍼: ATR ─────────────────────────────────────────
def calc_atr(high, low, close, period=14):
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# ── 헬퍼: OBV ─────────────────────────────────────────
def calc_obv(close, volume):
    direction = np.sign(close.diff())
    return (volume * direction).cumsum()

# ── 헬퍼: Stochastic ──────────────────────────────────
def calc_stochastic(high, low, close, k_period=14, d_period=3):
    lowest_low = low.rolling(k_period).min()
    highest_high = high.rolling(k_period).max()
    k = 100 * (close - lowest_low) / (highest_high - lowest_low)
    d = k.rolling(d_period).mean()
    return k, d

# ── 헬퍼: Williams %R ─────────────────────────────────
def calc_williams_r(high, low, close, period=14):
    highest_high = high.rolling(period).max()
    lowest_low = low.rolling(period).min()
    return -100 * (highest_high - close) / (highest_high - lowest_low)

# ── 헬퍼: MFI ─────────────────────────────────────────
def calc_mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3
    mf = tp * volume
    delta = tp.diff()
    pos_mf = mf.where(delta > 0, 0).rolling(period).sum()
    neg_mf = mf.where(delta <= 0, 0).rolling(period).sum()
    mfi = 100 - 100 / (1 + pos_mf / neg_mf.replace(0, np.nan))
    return mfi

# ── 헬퍼: CCI ─────────────────────────────────────────
def calc_cci(high, low, close, period=20):
    tp = (high + low + close) / 3
    ma = tp.rolling(period).mean()
    md = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean())
    return (tp - ma) / (0.015 * md)

# ── 헬퍼: ADX ─────────────────────────────────────────
def calc_adx(high, low, close, period=14):
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
    atr = calc_atr(high, low, close, period)
    plus_di = 100 * plus_dm.ewm(alpha=1/period).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1/period).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1/period).mean()
    return adx, plus_di, minus_di

# ── 티커 수집 ─────────────────────────────────────────
def get_all_tickers():
    """S&P500 + 나스닥 + 기타 소형주까지 최대한 수집"""
    tickers = set()

    # S&P 500
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        sp = list(tables[0]['Symbol'].str.replace('.', '-', regex=False))
        tickers.update(sp)
        log.info(f"S&P 500: {len(sp)} tickers")
    except Exception as e:
        log.warning(f"S&P 500 fetch failed: {e}")

    # NASDAQ-100
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        for t in tables:
            if 'Ticker' in t.columns:
                tickers.update(t['Ticker'].tolist())
                break
            elif 'Symbol' in t.columns:
                tickers.update(t['Symbol'].tolist())
                break
        log.info(f"After NASDAQ-100: {len(tickers)} tickers")
    except Exception as e:
        log.warning(f"NASDAQ-100 fetch failed: {e}")

    # Russell 2000 대용: 광범위 US 주식 목록
    try:
        import urllib.request
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        data = urllib.request.urlopen(url, timeout=30).read().decode()
        for line in data.strip().split('\n'):
            t = line.strip()
            if t and len(t) <= 5 and t.isalpha():
                tickers.add(t)
        log.info(f"After all_tickers: {len(tickers)} tickers")
    except Exception as e:
        log.warning(f"all_tickers fetch failed: {e}")

    # 추가: NYSE listed
    try:
        url2 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_tickers.txt"
        data2 = urllib.request.urlopen(url2, timeout=30).read().decode()
        for line in data2.strip().split('\n'):
            t = line.strip()
            if t and len(t) <= 5:
                tickers.add(t.replace('.', '-'))
        log.info(f"After NYSE: {len(tickers)} tickers")
    except:
        pass

    return sorted(tickers)


# ── 50개+ 지표 계산 ───────────────────────────────────
def compute_indicators(df, pos, row, ticker):
    """
    pos: surge day의 df 내 위치 인덱스
    row: surge day의 데이터
    lookback data = df.iloc[:pos] (surge일 이전까지)

    Returns dict with 50+ indicators
    """
    prev = df.iloc[max(0, pos-60):pos].copy()
    if len(prev) < 20:
        return None

    close = prev['Close']
    high = prev['High']
    low = prev['Low']
    volume = prev['Volume']
    opens = prev['Open']

    prev_close = close.iloc[-1]
    surge_date = df.index[pos]

    ind = {}

    # ── 기본 정보 (6개) ──
    ind['ticker'] = ticker
    ind['date'] = surge_date.strftime('%Y-%m-%d')
    ind['year'] = surge_date.year
    ind['month'] = surge_date.month
    ind['day_of_week'] = surge_date.dayofweek  # 0=Mon
    ind['quarter'] = (surge_date.month - 1) // 3 + 1

    # ── 급등일 가격 데이터 (8개) ──
    ind['surge_open'] = round(float(row['Open']), 4)
    ind['surge_high'] = round(float(row['High']), 4)
    ind['surge_low'] = round(float(row['Low']), 4)
    ind['surge_close'] = round(float(row['Close']), 4)
    ind['surge_volume'] = int(row['Volume'])
    ind['prev_close'] = round(float(prev_close), 4)
    ind['daily_return_pct'] = round(float((row['Close'] / prev_close - 1) * 100), 2)
    ind['open_gap_pct'] = round(float((row['Open'] / prev_close - 1) * 100), 2)

    # ── 가격 수준 지표 (6개) ──
    ind['price_before_surge'] = round(float(prev_close), 4)
    ind['is_penny_stock'] = 1 if prev_close < 5 else 0
    ind['is_micro_price'] = 1 if prev_close < 1 else 0
    ind['is_sub_10'] = 1 if prev_close < 10 else 0
    ind['price_bucket'] = (
        '<$1' if prev_close < 1 else
        '$1-5' if prev_close < 5 else
        '$5-10' if prev_close < 10 else
        '$10-20' if prev_close < 20 else
        '$20-50' if prev_close < 50 else
        '$50+'
    )
    ind['intraday_range_pct'] = round(float((row['High'] - row['Low']) / row['Low'] * 100), 2)

    # ── 거래량 지표 (8개) ──
    avg_vol_5 = volume.tail(5).mean()
    avg_vol_10 = volume.tail(10).mean()
    avg_vol_20 = volume.tail(20).mean()
    ind['avg_vol_5d'] = int(avg_vol_5)
    ind['avg_vol_10d'] = int(avg_vol_10)
    ind['avg_vol_20d'] = int(avg_vol_20)
    ind['surge_vol_vs_5d'] = round(float(row['Volume'] / max(avg_vol_5, 1)), 2)
    ind['surge_vol_vs_20d'] = round(float(row['Volume'] / max(avg_vol_20, 1)), 2)
    ind['is_low_vol_before'] = 1 if avg_vol_20 < 100000 else 0
    ind['is_very_low_vol'] = 1 if avg_vol_20 < 50000 else 0
    ind['vol_trend_5d'] = round(float(avg_vol_5 / max(avg_vol_20, 1) - 1) * 100, 2)

    # ── 수익률 지표 (사전 추세, 8개) ──
    if len(close) >= 3:
        ind['ret_1d_before'] = round(float((close.iloc[-1] / close.iloc[-2] - 1) * 100), 2)
    else:
        ind['ret_1d_before'] = 0
    if len(close) >= 5:
        ind['ret_3d_before'] = round(float((close.iloc[-1] / close.iloc[-3] - 1) * 100), 2)
    else:
        ind['ret_3d_before'] = 0
    if len(close) >= 6:
        ind['ret_5d_before'] = round(float((close.iloc[-1] / close.iloc[-5] - 1) * 100), 2)
    else:
        ind['ret_5d_before'] = 0
    if len(close) >= 11:
        ind['ret_10d_before'] = round(float((close.iloc[-1] / close.iloc[-10] - 1) * 100), 2)
    else:
        ind['ret_10d_before'] = 0
    if len(close) >= 21:
        ind['ret_20d_before'] = round(float((close.iloc[-1] / close.iloc[-20] - 1) * 100), 2)
    else:
        ind['ret_20d_before'] = 0
    if len(close) >= 41:
        ind['ret_40d_before'] = round(float((close.iloc[-1] / close.iloc[-40] - 1) * 100), 2)
    else:
        ind['ret_40d_before'] = None
    if len(close) >= 60:
        ind['ret_60d_before'] = round(float((close.iloc[-1] / close.iloc[-60] - 1) * 100), 2)
    else:
        ind['ret_60d_before'] = None

    # 최근 5일간 하락폭
    ind['max_drawdown_5d'] = round(float(
        (close.tail(5).min() / close.tail(5).max() - 1) * 100
    ), 2)

    # ── 변동성 지표 (6개) ──
    daily_rets = close.pct_change().dropna()
    ind['volatility_5d'] = round(float(daily_rets.tail(5).std() * 100), 2) if len(daily_rets) >= 5 else None
    ind['volatility_10d'] = round(float(daily_rets.tail(10).std() * 100), 2) if len(daily_rets) >= 10 else None
    ind['volatility_20d'] = round(float(daily_rets.tail(20).std() * 100), 2) if len(daily_rets) >= 20 else None

    atr_series = calc_atr(high, low, close, 14)
    if not atr_series.empty and not np.isnan(atr_series.iloc[-1]):
        ind['atr_14'] = round(float(atr_series.iloc[-1]), 4)
        ind['atr_pct_of_price'] = round(float(atr_series.iloc[-1] / max(prev_close, 0.01) * 100), 2)
    else:
        ind['atr_14'] = None
        ind['atr_pct_of_price'] = None
    ind['avg_intraday_range_20d'] = round(float(((high - low) / low * 100).tail(20).mean()), 2)

    # ── RSI 지표 (4개) ──
    rsi7 = calc_rsi(close, 7)
    rsi14 = calc_rsi(close, 14)
    ind['rsi_7'] = round(float(rsi7.iloc[-1]), 2) if not np.isnan(rsi7.iloc[-1]) else None
    ind['rsi_14'] = round(float(rsi14.iloc[-1]), 2) if not np.isnan(rsi14.iloc[-1]) else None
    ind['rsi_7_oversold'] = 1 if ind['rsi_7'] is not None and ind['rsi_7'] < 30 else 0
    ind['rsi_14_oversold'] = 1 if ind['rsi_14'] is not None and ind['rsi_14'] < 30 else 0

    # ── 볼린저 밴드 (4개) ──
    bb_ma, bb_upper, bb_lower, bb_width, bb_pos = calc_bollinger(close)
    if not bb_width.empty and not np.isnan(bb_width.iloc[-1]):
        ind['bb_width'] = round(float(bb_width.iloc[-1]), 2)
        ind['bb_position'] = round(float(bb_pos.iloc[-1]), 4)
        ind['below_lower_bb'] = 1 if bb_pos.iloc[-1] < 0 else 0
        ind['bb_squeeze'] = 1 if bb_width.iloc[-1] < bb_width.tail(20).quantile(0.2) else 0
    else:
        ind['bb_width'] = ind['bb_position'] = ind['below_lower_bb'] = ind['bb_squeeze'] = None

    # ── MACD (3개) ──
    macd, macd_signal, macd_hist = calc_macd(close)
    if not macd.empty and not np.isnan(macd.iloc[-1]):
        ind['macd'] = round(float(macd.iloc[-1]), 4)
        ind['macd_signal'] = round(float(macd_signal.iloc[-1]), 4)
        ind['macd_histogram'] = round(float(macd_hist.iloc[-1]), 4)
    else:
        ind['macd'] = ind['macd_signal'] = ind['macd_histogram'] = None

    # ── Stochastic (2개) ──
    stoch_k, stoch_d = calc_stochastic(high, low, close)
    if not stoch_k.empty and not np.isnan(stoch_k.iloc[-1]):
        ind['stoch_k'] = round(float(stoch_k.iloc[-1]), 2)
        ind['stoch_d'] = round(float(stoch_d.iloc[-1]), 2)
    else:
        ind['stoch_k'] = ind['stoch_d'] = None

    # ── Williams %R (1개) ──
    wr = calc_williams_r(high, low, close)
    ind['williams_r'] = round(float(wr.iloc[-1]), 2) if not np.isnan(wr.iloc[-1]) else None

    # ── MFI (1개) ──
    mfi = calc_mfi(high, low, close, volume)
    ind['mfi_14'] = round(float(mfi.iloc[-1]), 2) if not mfi.empty and not np.isnan(mfi.iloc[-1]) else None

    # ── CCI (1개) ──
    cci = calc_cci(high, low, close)
    ind['cci_20'] = round(float(cci.iloc[-1]), 2) if not cci.empty and not np.isnan(cci.iloc[-1]) else None

    # ── ADX (1개) ──
    adx, plus_di, minus_di = calc_adx(high, low, close)
    ind['adx_14'] = round(float(adx.iloc[-1]), 2) if not adx.empty and not np.isnan(adx.iloc[-1]) else None

    # ── OBV 추세 (2개) ──
    obv = calc_obv(close, volume)
    if len(obv) >= 5:
        ind['obv_5d_slope'] = round(float((obv.iloc[-1] - obv.iloc[-5]) / max(abs(obv.iloc[-5]), 1) * 100), 2)
    else:
        ind['obv_5d_slope'] = None
    if len(obv) >= 20:
        ind['obv_20d_slope'] = round(float((obv.iloc[-1] - obv.iloc[-20]) / max(abs(obv.iloc[-20]), 1) * 100), 2)
    else:
        ind['obv_20d_slope'] = None

    # ── 이동평균 관계 (5개) ──
    sma5 = close.tail(5).mean()
    sma10 = close.tail(10).mean()
    sma20 = close.tail(20).mean()
    sma50 = close.tail(50).mean() if len(close) >= 50 else None

    ind['dist_from_sma5_pct'] = round(float((prev_close / sma5 - 1) * 100), 2)
    ind['dist_from_sma10_pct'] = round(float((prev_close / sma10 - 1) * 100), 2)
    ind['dist_from_sma20_pct'] = round(float((prev_close / sma20 - 1) * 100), 2)
    ind['dist_from_sma50_pct'] = round(float((prev_close / sma50 - 1) * 100), 2) if sma50 else None
    ind['below_all_sma'] = 1 if (prev_close < sma5 and prev_close < sma10 and prev_close < sma20) else 0

    # ── 고/저점 거리 (4개) ──
    high_20d = high.tail(20).max()
    low_20d = low.tail(20).min()
    high_52w = high.max() if len(high) >= 200 else high.max()
    low_52w = low.min() if len(low) >= 200 else low.min()

    ind['dist_from_20d_high_pct'] = round(float((prev_close / high_20d - 1) * 100), 2)
    ind['dist_from_20d_low_pct'] = round(float((prev_close / low_20d - 1) * 100), 2)
    ind['dist_from_period_high_pct'] = round(float((prev_close / high_52w - 1) * 100), 2)
    ind['dist_from_period_low_pct'] = round(float((prev_close / low_52w - 1) * 100), 2)

    # ── 패턴 지표 (5개) ──
    # 연속 하락일
    down_days = 0
    for i in range(len(close)-1, 0, -1):
        if close.iloc[i] < close.iloc[i-1]:
            down_days += 1
        else:
            break
    ind['consecutive_down_days'] = down_days

    # 연속 상승일
    up_days = 0
    for i in range(len(close)-1, 0, -1):
        if close.iloc[i] > close.iloc[i-1]:
            up_days += 1
        else:
            break
    ind['consecutive_up_days'] = up_days

    # 갭다운 여부 (전일)
    if len(opens) >= 2 and len(close) >= 2:
        ind['prev_day_gap_down'] = 1 if opens.iloc[-1] < close.iloc[-2] * 0.97 else 0
    else:
        ind['prev_day_gap_down'] = 0

    # 최근 5일간 양봉 비율
    if len(close) >= 5 and len(opens) >= 5:
        green_candles = sum(1 for i in range(-5, 0) if close.iloc[i] > opens.iloc[i])
        ind['green_candle_ratio_5d'] = round(green_candles / 5, 2)
    else:
        ind['green_candle_ratio_5d'] = None

    # 급등일 갭업 규모
    ind['gap_up_on_surge'] = round(float((row['Open'] / prev_close - 1) * 100), 2)

    # ── 급등 후 데이터 (5개) ── (가능한 경우)
    post = df.iloc[pos+1:min(pos+6, len(df))]
    if len(post) >= 1:
        ind['next_1d_return'] = round(float((post['Close'].iloc[0] / row['Close'] - 1) * 100), 2)
    else:
        ind['next_1d_return'] = None
    if len(post) >= 3:
        ind['next_3d_return'] = round(float((post['Close'].iloc[2] / row['Close'] - 1) * 100), 2)
    else:
        ind['next_3d_return'] = None
    if len(post) >= 5:
        ind['next_5d_return'] = round(float((post['Close'].iloc[4] / row['Close'] - 1) * 100), 2)
        ind['next_5d_max_drawdown'] = round(float((post['Low'].head(5).min() / row['Close'] - 1) * 100), 2)
        ind['next_5d_max_gain'] = round(float((post['High'].head(5).max() / row['Close'] - 1) * 100), 2)
    else:
        ind['next_5d_return'] = ind['next_5d_max_drawdown'] = ind['next_5d_max_gain'] = None

    return ind


# ── 메인 ──────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.info("=" * 60)
    log.info("200%+ Daily Surge Collector & Analyzer")
    log.info("=" * 60)

    # 1) 티커 수집
    log.info("[1/4] 티커 수집 중...")
    all_tickers = get_all_tickers()
    log.info(f"총 {len(all_tickers)}개 티커")

    # 2) 배치 다운로드 + 200%+ 필터
    log.info(f"[2/4] 200%+ 급등 스캔 중 ({START_DATE} ~ {END_DATE})...")
    events = []
    total_batches = (len(all_tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    scanned_tickers = 0
    failed_tickers = 0

    for batch_idx in range(total_batches):
        b_start = batch_idx * BATCH_SIZE
        b_end = min(b_start + BATCH_SIZE, len(all_tickers))
        batch = all_tickers[b_start:b_end]

        if batch_idx % 10 == 0:
            log.info(f"  배치 {batch_idx+1}/{total_batches} | 스캔: {scanned_tickers} | 급등: {len(events)}")

        try:
            # 지표 계산을 위해 60일 더 일찍 시작
            fetch_start = (pd.Timestamp(START_DATE) - timedelta(days=120)).strftime('%Y-%m-%d')
            data = yf.download(
                " ".join(batch),
                start=fetch_start, end=END_DATE,
                group_by='ticker', progress=False, threads=True
            )

            if data.empty:
                continue

            for ticker in batch:
                try:
                    if len(batch) == 1:
                        df = data.copy()
                    else:
                        if ticker not in data.columns.get_level_values(0):
                            continue
                        df = data[ticker].copy()

                    df = df.dropna(subset=['Close', 'Open', 'High', 'Low', 'Volume'])
                    if len(df) < MIN_HISTORY_DAYS:
                        continue

                    scanned_tickers += 1

                    # 일별 수익률 계산
                    df['prev_close'] = df['Close'].shift(1)
                    df['daily_ret_pct'] = (df['Close'] / df['prev_close'] - 1) * 100

                    # 분석 기간 내에서만 필터
                    analysis_mask = (df.index >= pd.Timestamp(START_DATE)) & (df.index <= pd.Timestamp(END_DATE))
                    surge_mask = analysis_mask & (df['daily_ret_pct'] >= SURGE_THRESHOLD)

                    for idx in df[surge_mask].index:
                        pos = df.index.get_loc(idx)
                        if pos < MIN_HISTORY_DAYS:
                            continue

                        row = df.iloc[pos]
                        indicators = compute_indicators(df, pos, row, ticker)
                        if indicators:
                            events.append(indicators)

                except Exception:
                    failed_tickers += 1
                    continue

        except Exception:
            continue

    log.info(f"  스캔 완료: {scanned_tickers}개 티커 | 실패: {failed_tickers}")
    log.info(f"  200%+ 급등 이벤트: {len(events)}건")

    if not events:
        log.warning("급등 이벤트 0건 — 종료")
        summary = {"total_events": 0, "note": "No 200%+ surge events found"}
        with open(f"{OUTPUT_DIR}/surge_200pct_summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        return

    # 3) DataFrame 저장
    log.info("[3/4] CSV 저장 중...")
    df_events = pd.DataFrame(events)
    df_events.to_csv(f"{OUTPUT_DIR}/surge_200pct_events.csv", index=False)
    log.info(f"  → {OUTPUT_DIR}/surge_200pct_events.csv ({len(df_events)} rows, {len(df_events.columns)} columns)")

    # 4) 공통점 분석
    log.info("[4/4] 공통점 분석 중...")

    summary = {
        "total_events": len(df_events),
        "unique_tickers": int(df_events['ticker'].nunique()),
        "date_range": f"{START_DATE} ~ {END_DATE}",
        "tickers_scanned": scanned_tickers,
        "columns_count": len(df_events.columns),
    }

    # 연도별 분포
    year_dist = df_events.groupby('year').size().to_dict()
    summary["by_year"] = {str(k): int(v) for k, v in year_dist.items()}

    # 월별 분포
    month_dist = df_events.groupby('month').size().to_dict()
    summary["by_month"] = {str(k): int(v) for k, v in month_dist.items()}

    # 요일별 분포
    dow_names = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri'}
    dow_dist = df_events.groupby('day_of_week').size().to_dict()
    summary["by_day_of_week"] = {dow_names.get(k, str(k)): int(v) for k, v in dow_dist.items()}

    # 가격대별 분포
    price_dist = df_events.groupby('price_bucket').size().to_dict()
    summary["by_price_bucket"] = {str(k): int(v) for k, v in price_dist.items()}

    # 수치형 지표 통계
    numeric_cols = df_events.select_dtypes(include=[np.number]).columns
    stats = {}
    for col in numeric_cols:
        if col in ('year', 'month', 'day_of_week', 'quarter'):
            continue
        s = df_events[col].dropna()
        if len(s) == 0:
            continue
        stats[col] = {
            'mean': round(float(s.mean()), 4),
            'median': round(float(s.median()), 4),
            'std': round(float(s.std()), 4),
            'min': round(float(s.min()), 4),
            'max': round(float(s.max()), 4),
            'q25': round(float(s.quantile(0.25)), 4),
            'q75': round(float(s.quantile(0.75)), 4),
            'pct_positive': round(float((s > 0).mean() * 100), 1),
        }
    summary["indicator_stats"] = stats

    # ── 공통점 패턴 추출 ──
    patterns = {}
    total = len(df_events)

    def pct(count): return round(count / total * 100, 1)

    # 페니 주식 비율
    penny = int(df_events['is_penny_stock'].sum())
    patterns['penny_stock_<$5'] = {"count": penny, "pct": pct(penny)}

    micro = int(df_events['is_micro_price'].sum())
    patterns['micro_price_<$1'] = {"count": micro, "pct": pct(micro)}

    sub10 = int(df_events['is_sub_10'].sum())
    patterns['under_$10'] = {"count": sub10, "pct": pct(sub10)}

    # RSI 과매도
    rsi_os = int(df_events['rsi_14_oversold'].sum())
    patterns['rsi14_oversold_<30'] = {"count": rsi_os, "pct": pct(rsi_os)}

    # 볼린저 하단 이탈
    bb_below = int(df_events['below_lower_bb'].dropna().sum())
    patterns['below_lower_bb'] = {"count": bb_below, "pct": pct(bb_below)}

    # 저거래량
    low_vol = int(df_events['is_low_vol_before'].sum())
    patterns['low_vol_before_<100K'] = {"count": low_vol, "pct": pct(low_vol)}

    # 연속 하락
    consec_down_3 = int((df_events['consecutive_down_days'] >= 3).sum())
    patterns['3+_consecutive_down_days'] = {"count": consec_down_3, "pct": pct(consec_down_3)}

    # 갭업 급등
    gap_100 = int((df_events['gap_up_on_surge'] >= 100).sum())
    patterns['gap_up_100%+_on_surge'] = {"count": gap_100, "pct": pct(gap_100)}

    # 20일 고점 대비 크게 하락 후 급등
    dist_20d = df_events['dist_from_20d_high_pct'].dropna()
    down_50 = int((dist_20d <= -50).sum())
    patterns['50%+_below_20d_high'] = {"count": down_50, "pct": pct(down_50)}

    # SMA 아래
    below_sma = int(df_events['below_all_sma'].sum())
    patterns['below_all_sma_5_10_20'] = {"count": below_sma, "pct": pct(below_sma)}

    # 급등 후 반락
    if 'next_1d_return' in df_events.columns:
        next1d = df_events['next_1d_return'].dropna()
        crash_next = int((next1d < -20).sum())
        patterns['crash_>20%_next_day'] = {"count": crash_next, "pct": round(crash_next / len(next1d) * 100, 1) if len(next1d) > 0 else 0}

    # 거래량 폭발 (10배+)
    vol_exp = df_events['surge_vol_vs_20d'].dropna()
    vol_10x = int((vol_exp >= 10).sum())
    patterns['volume_10x+_on_surge'] = {"count": vol_10x, "pct": pct(vol_10x)}

    summary["common_patterns"] = patterns

    # ── 복합 공통점 (여러 조건 동시) ──
    compound = {}

    # 페니 + 저거래량 + SMA 아래
    mask1 = (df_events['is_penny_stock'] == 1) & (df_events['is_low_vol_before'] == 1) & (df_events['below_all_sma'] == 1)
    compound['penny_AND_low_vol_AND_below_sma'] = {"count": int(mask1.sum()), "pct": pct(mask1.sum())}

    # 가격 < $1 + 거래량 10배+ + 갭업 100%+
    mask2 = (df_events['is_micro_price'] == 1) & (df_events['surge_vol_vs_20d'] >= 10)
    gap_up_series = df_events['gap_up_on_surge'] >= 100
    mask2 = mask2 & gap_up_series
    compound['micro_AND_vol10x_AND_gap100'] = {"count": int(mask2.sum()), "pct": pct(mask2.sum())}

    # 3일 연속 하락 + RSI 과매도
    mask3 = (df_events['consecutive_down_days'] >= 3) & (df_events['rsi_14_oversold'] == 1)
    compound['3d_down_AND_rsi_oversold'] = {"count": int(mask3.sum()), "pct": pct(mask3.sum())}

    summary["compound_patterns"] = compound

    # 저장
    with open(f"{OUTPUT_DIR}/surge_200pct_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    log.info(f"  → {OUTPUT_DIR}/surge_200pct_summary.json")

    # 콘솔 요약
    log.info("\n" + "=" * 60)
    log.info(f"총 이벤트: {total}건 | 티커: {summary['unique_tickers']}개")
    log.info(f"컬럼(지표) 수: {summary['columns_count']}개")
    log.info("-" * 40)
    log.info("연도별:")
    for y, c in sorted(summary['by_year'].items()):
        log.info(f"  {y}: {c}건")
    log.info("-" * 40)
    log.info("주요 공통점:")
    for name, data in sorted(patterns.items(), key=lambda x: x[1]['pct'], reverse=True):
        log.info(f"  {name}: {data['count']}건 ({data['pct']}%)")
    log.info("-" * 40)
    log.info("복합 패턴:")
    for name, data in compound.items():
        log.info(f"  {name}: {data['count']}건 ({data['pct']}%)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
