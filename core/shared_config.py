#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Shared Configuration
  공통 상수, 유틸리티 함수 (scanner / tracker / optimizer 공유)
================================================================================
"""

from datetime import datetime, date, timedelta, timezone

# ─── Timezone ────────────────────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))

# ─── Leveraged ETF Exclusion List ────────────────────────────────────────────
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

# ─── Strategy Configuration (v5) ─────────────────────────────────────────────
# v5: 전략 1·3만 운영. 2·4~10 은 제거(기록은 data/archive_removed_strategies/ 에 보관).
ACTIVE_STRATEGIES = ['1', '3']

# v6: 손절 -10%, 최대 2거래일 (최근 217건 1분봉 재계산: -20%/5일보다 건당 평균↑, -15% 넘는 손실 39건→5건)
STRATEGY_CONFIG = {
    '1': {'tp_pct': 0.05, 'sl_pct': -0.10, 'max_hold': 2},
    '3': {'tp_pct': 0.05, 'sl_pct': -0.10, 'max_hold': 2},
}

STRATEGY_NAMES = {
    '1': '1차_1_+5%5일 · 급락반등',
    '3': '1차_3_+5%5일 · 과매도',
}

# ─── Execution Model (v6) ────────────────────────────────────────────────────
# 장 마감 30분 전(15:30 ET)에 감시목록을 빠르게 확인해 신호를 내고, 그날 '마감가'(종가 주문)에 진입.
# 아침에 신호를 보고 사면(애프터·프리·다음날 시가) 이미 평균 2~4% 올라 있어 성적이 나빠진다(217건 1분봉 분석).
ENTRY_MODE = 'signal_close'
PRECLOSE_SCAN_ET = (15, 30)          # 마감 전 스캔 시각 (ET)
WATCHLIST_RSI7 = 45                  # 전날 RSI7 < 45
WATCHLIST_CONSEC = 3                 # 전날 연속하락 ≥ 3  → 하루 평균 ~450종목, 실제 신호의 95% 포함
# 왕복 거래비용(수수료+호가차이+슬리피지). 결과(result_pct)에서 차감된다.
COST_PCT = 1.0
# 실험 지표: 신호일 종가에 사서 다음날 시가에 판다면? (일봉 기준, 호가 차이 착시 가능 → 참고용)
TRACK_OVERNIGHT = True

# ─── Common Filters ──────────────────────────────────────────────────────────
MIN_PRICE = 1.0
MIN_VOLUME = 10000       # 20일 평균거래량 기준
BATCH_SIZE = 80
BATCH_DELAY = 1.5

DATA_DIR = 'data'


# ─── US DST / Holiday Utilities ──────────────────────────────────────────────

def is_us_dst(dt_date):
    """
    미국 DST 여부 확인.
    DST: 3월 두 번째 일요일 02:00 ~ 11월 첫 번째 일요일 02:00
    """
    year = dt_date.year

    # 3월 두 번째 일요일
    mar1 = date(year, 3, 1)
    days_to_sun = (6 - mar1.weekday()) % 7
    dst_start = mar1 + timedelta(days=days_to_sun + 7)

    # 11월 첫 번째 일요일
    nov1 = date(year, 11, 1)
    days_to_sun = (6 - nov1.weekday()) % 7
    dst_end = nov1 + timedelta(days=days_to_sun)

    d = dt_date if isinstance(dt_date, date) else dt_date.date()
    return dst_start <= d < dst_end


def get_us_market_holidays(year):
    """
    미국 주식시장 주요 공휴일 목록 (NYSE 기준).
    주말에 걸리면 금요일/월요일로 대체.
    """
    holidays = set()

    # New Year's Day (1/1)
    holidays.add(date(year, 1, 1))

    # MLK Day (3rd Monday of January)
    jan1 = date(year, 1, 1)
    first_mon = jan1 + timedelta(days=(0 - jan1.weekday()) % 7)
    if first_mon < jan1:
        first_mon += timedelta(days=7)
    holidays.add(first_mon + timedelta(weeks=2))

    # Presidents' Day (3rd Monday of February)
    feb1 = date(year, 2, 1)
    first_mon = feb1 + timedelta(days=(0 - feb1.weekday()) % 7)
    if first_mon < feb1:
        first_mon += timedelta(days=7)
    holidays.add(first_mon + timedelta(weeks=2))

    # Memorial Day (last Monday of May)
    may31 = date(year, 5, 31)
    mem = may31 - timedelta(days=(may31.weekday() - 0) % 7)
    holidays.add(mem)

    # Juneteenth (6/19)
    holidays.add(date(year, 6, 19))

    # Independence Day (7/4)
    holidays.add(date(year, 7, 4))

    # Labor Day (1st Monday of September)
    sep1 = date(year, 9, 1)
    first_mon = sep1 + timedelta(days=(0 - sep1.weekday()) % 7)
    if first_mon < sep1:
        first_mon += timedelta(days=7)
    holidays.add(first_mon)

    # Thanksgiving (4th Thursday of November)
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    if first_thu < nov1:
        first_thu += timedelta(days=7)
    holidays.add(first_thu + timedelta(weeks=3))

    # Christmas (12/25)
    holidays.add(date(year, 12, 25))

    # 주말 대체 처리
    adjusted = set()
    for h in holidays:
        if h.weekday() == 5:      # 토요일 → 금요일
            adjusted.add(h - timedelta(days=1))
        elif h.weekday() == 6:    # 일요일 → 월요일
            adjusted.add(h + timedelta(days=1))
        else:
            adjusted.add(h)

    return adjusted


def is_us_trading_day(dt_date):
    """해당 날짜가 미국 시장 거래일인지 확인 (주말 + 공휴일 제외)"""
    d = dt_date if isinstance(dt_date, date) else dt_date.date()
    if d.weekday() >= 5:
        return False
    holidays = get_us_market_holidays(d.year)
    return d not in holidays


def get_last_trading_date(from_date):
    """from_date 이전(미포함)의 마지막 거래일 반환"""
    d = from_date - timedelta(days=1)
    while not is_us_trading_day(d):
        d -= timedelta(days=1)
    return d


def get_expected_trading_date():
    """
    현재 UTC 시각 기준으로, yfinance에서 기대되는 최신 거래일을 반환.
    DST를 고려하여 장마감 시각을 정확히 판단.
    - EDT (summer): 장마감 = UTC 20:00 → 데이터 기대 = UTC 20:30 이후
    - EST (winter): 장마감 = UTC 21:00 → 데이터 기대 = UTC 21:30 이후
    """
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date()

    # DST 여부에 따라 장마감 시각 결정
    if is_us_dst(today_utc):
        close_hour, close_min = 20, 30   # EDT: UTC 20:30 이후
    else:
        close_hour, close_min = 21, 30   # EST: UTC 21:30 이후

    market_closed = (now_utc.hour > close_hour or
                     (now_utc.hour == close_hour and now_utc.minute >= close_min))

    if market_closed and is_us_trading_day(today_utc):
        return today_utc
    else:
        return get_last_trading_date(today_utc)


# ─── Shared RSI Calculator (Wilder Method) ──────────────────────────────────

def calc_rsi_wilder(close_series, period=7):
    """
    Wilder RSI (SMA seed + Wilder smoothing).
    scanner.py / tracker.py / backtest_optimizer.py 모두 이 함수를 사용.
    """
    import pandas as pd
    import numpy as np

    delta = close_series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    for i in range(period, len(close_series)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))


# ─── Shared Ticker Fetcher ───────────────────────────────────────────────────

def get_all_tickers():
    """NASDAQ/NYSE/AMEX 전체 상장 종목 수집"""
    import requests

    tickers = set()
    excluded_etf = set()
    urls = [
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                lines = resp.text.strip().split('\n')
                header = [h.strip() for h in lines[0].split('|')]
                sym_col = 'Symbol' if 'Symbol' in header else ('ACT Symbol' if 'ACT Symbol' in header else None)
                etf_col = header.index('ETF') if 'ETF' in header else None
                test_col = header.index('Test Issue') if 'Test Issue' in header else None
                for line in lines[1:]:
                    parts = line.split('|')
                    if sym_col is None or len(parts) < len(header):
                        continue
                    sym = parts[header.index(sym_col)].strip()
                    # v5: ETF·ETN·레버리지 상품과 테스트 종목은 제외 (전략은 개별 주식 대상)
                    if etf_col is not None and parts[etf_col].strip() == 'Y':
                        excluded_etf.add(sym)
                        continue
                    if test_col is not None and parts[test_col].strip() == 'Y':
                        continue
                    if sym and sym.isalpha() and 1 <= len(sym) <= 5 and sym != 'Symbol':
                        tickers.add(sym)
                print(f"  [{url.split('/')[-1]}] {len(tickers)} tickers")
        except requests.RequestException as e:
            print(f"  Warning: {url.split('/')[-1]}: {e}")

    if len(tickers) < 1000:
        try:
            resp = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers={"User-Agent": "SurgeScanner/2.0 scanner@example.com"},
                timeout=15,
            )
            if resp.status_code == 200:
                for item in resp.json().values():
                    sym = item.get('ticker', '').strip()
                    if sym and 1 <= len(sym) <= 5 and sym.isalpha():
                        tickers.add(sym)
                print(f"  [SEC fallback] {len(tickers)} tickers")
        except requests.RequestException as e:
            print(f"  Warning: SEC fallback failed: {e}")

    tickers -= LEVERAGED_ETF
    tickers -= excluded_etf   # 다른 목록에서 ETF 표시 없이 들어온 경우도 제외
    tickers = {t for t in tickers if not t.endswith('W') and not any(c.isdigit() for c in t)}
    print(f"  Total: {len(tickers)} tickers (ETF/ETN {len(excluded_etf)}개 제외)")
    return sorted(tickers)


# ─── Shared yfinance Batch Download ─────────────────────────────────────────

def download_batch(tickers, period='60d'):
    """yfinance 배치 다운로드 (재시도 포함)"""
    import yfinance as yf
    import time

    for attempt in range(3):
        try:
            data = yf.download(
                ' '.join(tickers), period=period,
                group_by='ticker', progress=False, threads=True, timeout=30,
            )
            return data
        except Exception as e:
            if attempt < 2:
                print(f"    Download retry {attempt+1}/3: {e}")
                time.sleep(3)
            else:
                print(f"    Download failed after 3 attempts: {e}")
    return None


def extract_ticker_df(data, tk, batch_size):
    """yfinance 반환값에서 특정 티커의 DataFrame을 안전하게 추출."""
    import pandas as pd

    if data is None or data.empty:
        return None

    cols = data.columns

    # Case 1: 단일 인덱스
    if not isinstance(cols, pd.MultiIndex):
        if batch_size == 1:
            return data.dropna(how='all')
        return None

    # Case 2: MultiIndex 컬럼
    level_values = [set(cols.get_level_values(i)) for i in range(cols.nlevels)]

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
