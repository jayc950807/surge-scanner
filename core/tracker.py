#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Position Tracker (v6)

  [v6 판정 규칙 — research/STRATEGY_V6.md]
    진입: 신호일(signal_date) 마감가 (entry_date = signal_date, entry_price = 그날 종가).
          마감 전(15:30 ET) 스캔에서 잡힌 신호만 추적한다(scan_type='preclose').
          신호일 일봉이 아직 확정되지 않았으면 PENDING 으로 남겨 두고 다음 실행 때 진입한다.
    추적: 진입 다음 거래일부터 max_hold(2) 거래일. 1분봉이 있으면 분 단위로(judge='minute'),
          없으면 일봉으로(judge='daily') 판정한다.
      1. WIN     : TP(+5%) 먼저 도달 → tp_price 체결 (시가가 이미 TP 이상이면 시가 체결)
      2. LOSS    : SL(-10%) 먼저 도달 → sl_price 체결 (시가가 이미 SL 이하로 갭하락하면 시가 체결 — 손실이 더 큼)
                   일봉 판정에서 같은 날 TP·SL 동시 도달은 선후를 알 수 없으므로 보수적으로 LOSS
      3. EXPIRED : max_hold 경과 → 마지막 날 종가 청산
    비용: result_pct = 가격 손익 − COST_PCT(왕복). gross_pct 에 비용 전 값을 따로 남긴다.
    실험 지표: overnight_pct = (다음 거래일 시가 / 진입가 − 1) − COST_PCT (기록용)
    method 열: v6_close_entry (현재 규칙) / v5_next_open, v4_signal_close (예전 규칙으로 계산돼 다시 계산 못 한 기록 —
              화면 통계에서 제외)

  Usage:
    python tracker.py                # 전체 업데이트 (최초 1회 전체 기록을 v6 규칙으로 재계산)
    python tracker.py --register-only# 오늘 마감 전 신호를 PENDING 으로 등록만
    python tracker.py --init         # 기존 history.csv에서 포지션 초기 생성
    python tracker.py --reverify     # 모든 청산 내역 재검증 (가격 재조회)
================================================================================
"""

import pandas as pd
import numpy as np
import os
import json
import argparse
import time
import glob
import logging
from datetime import datetime, timedelta, timezone

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

# ─── Import from shared_config ────────────────────────────────────────────────
from shared_config import (
    STRATEGY_CONFIG, STRATEGY_NAMES, ACTIVE_STRATEGIES, COST_PCT, DATA_DIR, KST,
    get_expected_trading_date,
)
import alpaca_data

# ─── Column definitions ──────────────────────────────────────────────────────
OPEN_COLS = [
    'strategy', 'ticker', 'signal_date', 'signal_price',
    'entry_date', 'entry_price',
    'tp_price', 'sl_price', 'tp_pct', 'max_hold',
    'status',  # PENDING, OPEN
    'current_price', 'max_price', 'max_price_date',
    'min_price', 'min_price_date',
    'change_pct', 'achievement_pct',
    'days_held', 'last_updated',
    'overnight_pct', 'method', 'judge',
    'scan_type',   # preclose(마감 전 신호 — 실제로 살 수 있었던 것) / afterclose / '' (예전 기록)
]

CLOSED_COLS = OPEN_COLS + [
    'close_date', 'close_price', 'result_pct',
    'result_status',  # WIN, LOSS, EXPIRED  (v4: TRAILING 제거)
    'tp_hit_date',
    'max_achievement_pct',
    'gross_pct', 'cost_pct',
]
METHOD_V6 = 'v6_close_entry'     # 신호일 종가 진입 · 진입 다음 거래일부터 판정 · -10%/2일
METHOD_V5 = 'v5_next_open'
METHOD_V4 = 'v4_signal_close'
V6_MARKER = os.path.join('data', '.v6_reverified')

OPEN_PATH = os.path.join(DATA_DIR, 'open_positions.csv')
CLOSED_PATH = os.path.join(DATA_DIR, 'closed_positions.csv')


# ─── Legacy Strategy Code Migration ──────────────────────────────────────────
_LEGACY_STRAT_MAP = {'A': '1', 'B': '2', 'C': '3', 'D': '4', 'E': '5', 'F': '6', 'G': '7', 'H': '8', 'I': '9', 'J': '10'}

def _fix_legacy_strategy(df):
    """이전 버전 알파벳 전략 코드(A~J)를 숫자(1~10)로 변환"""
    if not df.empty and 'strategy' in df.columns:
        df['strategy'] = df['strategy'].replace(_LEGACY_STRAT_MAP)
    return df

# ─── Helper Functions ─────────────────────────────────────────────────────────

def load_csv(path, cols=None):
    """Load CSV file with optional column validation."""
    if os.path.exists(path):
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = ''
        _fix_legacy_strategy(df)
        # v5: 운영 중인 전략(1·3)만 다룬다
        if 'strategy' in df.columns and not df.empty:
            df = df[df['strategy'].astype(str).isin(ACTIVE_STRATEGIES)].reset_index(drop=True)
        return df
    if cols:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame()


def save_csv(df, path):
    """Save DataFrame to CSV."""
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(path, index=False)


def get_trading_days_between(start_date, end_date):
    """두 날짜 사이의 거래일 수 (주말 제외)"""
    days = pd.bdate_range(start=start_date, end=end_date)
    return len(days) - 1  # start_date 제외


def fetch_price_data(ticker, start_date, end_date=None):
    """Alpaca 일봉(분할 보정, 인덱스 = 날짜 naive). 없으면 None. (예전 야후 history() 와 같은 모양)"""
    try:
        df = alpaca_data.history_daily(ticker, start_date, end_date, adjustment='split')
        return df if df is not None and len(df) > 0 else None
    except Exception as e:
        logging.warning(f"{ticker} price fetch failed: {e}")
        return None


def get_entry_for_signal(ticker, signal_date):
    """
    [v6] 신호일 '종가'(마감가 주문)로 진입. entry_date = signal_date.
    반환: (entry_date, entry_price, signal_close)  — 둘 다 종가.
      - 신호일 일봉이 아직 없으면 (None, None, None) → PENDING 유지
    """
    try:
        sig_dt = pd.to_datetime(signal_date)
        # 장이 끝나기 전(수동 실행 등)에는 '종가'가 아직 없으므로 진입하지 않는다
        if sig_dt.date() > get_expected_trading_date():
            return None, None, None
        hist = fetch_price_data(ticker, signal_date)
        if hist is None or len(hist) == 0:
            return None, None, None
        on_sig = hist[hist.index.normalize() == sig_dt]
        if on_sig.empty:
            return None, None, None
        close = float(on_sig['Close'].iloc[0])
        if not close or close <= 0 or np.isnan(close):
            return None, None, None
        return sig_dt.strftime('%Y-%m-%d'), close, close
    except Exception as e:
        logging.warning(f"get_entry_for_signal({ticker}, {signal_date}) failed: {e}")
    return None, None, None


def fetch_minute_data(ticker, start_date):
    """start_date 다음 날부터 7일 창의 1분봉(정규장 09:30~16:00, 16:00 봉 = 종가 체결 포함, 분할 보정).
    Alpaca 는 과거 1분봉도 다 있어 오래된 기록도 분 단위로 판정할 수 있다 → 실패하면 None (일봉으로 대체)."""
    try:
        d0 = pd.to_datetime(start_date)
        df = alpaca_data.history_minute(ticker, (d0 + timedelta(days=1)).date(), (d0 + timedelta(days=8)).date(), adjustment='split')
        if df is None or df.empty:
            return None
        df = df[df.index.normalize() > d0]
        return df if len(df) else None
    except Exception as e:
        logging.warning(f"{ticker} minute fetch failed: {e}")
        return None


def minute_data_valid(mins, hist_tracking):
    """분봉의 첫 거래일이 진입 다음 거래일(일봉 첫 행)과 같을 때만 분봉 판정을 쓴다 (8일 창 밖이면 일봉으로)."""
    if mins is None or hist_tracking is None or hist_tracking.empty:
        return False
    return mins.index[0].normalize() == hist_tracking.index[0].normalize()


def track_position_intraday(entry_price, tp_price, sl_price, max_hold, mins, finished_days=None):
    """[v6] 1분봉으로 순서대로 판정 — 같은 날 ±가 둘 다 닿아도 어느 쪽이 먼저였는지 안다.
    둘째 날부터 시가 갭은 그 시가로 체결. 반환 형식은 track_position_daywise 와 같다."""
    days = sorted(set(mins.index.normalize()))[:max_hold]
    max_price, max_date, min_price, min_date = entry_price, '', entry_price, ''
    tp_pct_val = (tp_price - entry_price) / entry_price if entry_price > 0 else 0
    def done(status, px, d, dn):
        rp = (px - entry_price) / entry_price * 100
        ma = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
        return (status, px, d, d if status == 'WIN' else '', max_price, max_date, min_price, min_date, dn, rp, min(ma, 999) if status != 'WIN' else 100.0)
    for dn, day in enumerate(days, start=1):
        g = mins[mins.index.normalize() == day]; ds = day.strftime('%Y-%m-%d')
        for i, (o, h, l, c) in enumerate(zip(g['Open'], g['High'], g['Low'], g['Close'])):
            if h > max_price: max_price, max_date = h, ds
            if l < min_price: min_price, min_date = l, ds
            # 시가 갭 체결은 그날 첫 봉만: 장 시작 전 걸어 둔 주문이 시가에 체결되는 경우.
            # 장중에 거래가 띄엄띄엄해 다음 봉 시가가 목표가를 건너뛰어도, 걸어 둔 지정가 매도는 목표가에 체결된다.
            if i == 0 and sl_price > 0 and o <= sl_price: return done('LOSS', o, ds, dn)
            if i == 0 and tp_price > 0 and o >= tp_price: return done('WIN', o, ds, dn)
            if i > 0 and sl_price > 0 and o <= sl_price: return done('LOSS', o, ds, dn)   # 손절은 다음 체결가(봉 시가)로 — 보수적
            if sl_price > 0 and l <= sl_price: return done('LOSS', sl_price, ds, dn)
            if tp_price > 0 and h >= tp_price: return done('WIN', tp_price, ds, dn)
    # 마지막 보유일이 끝났는지: 그날 확정 일봉이 있거나(조기 마감·거래 없는 종목 대비) 15:55 이후 봉이 있으면 만기
    last = mins[mins.index.normalize() == days[-1]] if days else mins.iloc[0:0]
    day_done = (finished_days is not None and len(days) and days[-1].normalize() in set(finished_days)) or \
               (len(last) and (last.index[-1].hour * 60 + last.index[-1].minute) >= 955)
    if len(days) >= max_hold and len(last) and day_done:
        return done('EXPIRED', float(last['Close'].iloc[-1]), days[-1].strftime('%Y-%m-%d'), len(days))
    ma = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
    held = len(days) - (0 if day_done else 1)        # 아직 끝나지 않은 날은 보유일수에 세지 않음 (장중 실행 대비)
    return (None, float(mins['Close'].iloc[-1]), None, None, max_price, max_date, min_price, min_date, max(held, 0), None, min(ma, 999))


def drop_unfinished_bar(hist):
    """장 마감 전에 실행되면 오늘의 미완성 일봉을 판정에서 뺀다."""
    if hist is None or hist.empty:
        return hist
    try:
        last_done = pd.Timestamp(get_expected_trading_date())
        return hist[hist.index.normalize() <= last_done]
    except Exception:
        return hist


def safe_int(v, default):
    try:
        x = float(v)
        return int(x) if not np.isnan(x) else default
    except (TypeError, ValueError):
        return default


CLOSE_FIELDS = ['close_date', 'close_price', 'result_pct', 'result_status',
                'tp_hit_date', 'gross_pct', 'cost_pct', 'max_achievement_pct']


def net_of_cost(gross_pct):
    """가격 손익(%)에서 왕복 비용(%)을 뺀 값"""
    return gross_pct - COST_PCT


def track_position_daywise(entry_price, tp_price, sl_price, max_hold, hist_after_entry, gap_from_first_bar=False):
    """
    일봉으로 하루씩 TP/SL 도달 여부 검사.
    v6(gap_from_first_bar=True): 첫 행 = 진입(종가) 다음 거래일 → 첫 행부터 시가 갭을 확인한다.
    v5(gap_from_first_bar=False): 첫 행 = 진입일(시가 매수) → 둘째 날부터 시가 갭을 확인한다.
    (갭하락 → 시가 손절, 갭상승 → 시가 익절)
    result_pct 는 비용 전 가격 손익(%)이다 — 비용 차감은 호출한 쪽에서 한다.

    판정 규칙 (반드시 코드와 1:1 일치):
      - max_hold 이내에 TP(High >= tp_price)에 먼저 도달       → WIN
      - max_hold 이내에 SL(Low  <= sl_price)에 먼저 도달       → LOSS
      - 같은 날 TP와 SL이 모두 도달 (High>=tp_price AND
        Low<=sl_price): 일봉 데이터만으로는 장중 선후 관계를
        확정할 수 없다. 백테스트 결과의 과신을 막기 위해
        **보수적으로 LOSS로 판정한다.**
      - max_hold 경과 시까지 TP·SL 모두 미도달                 → EXPIRED
      - 위 어느 조건도 발동하지 않고 데이터가 부족해 아직
        결정되지 않은 경우                                     → None (진행중)

    ※ Trailing stop 관련 로직은 v4에서 전면 제거됨 (trailing_pct, trailing_level,
       peak_price 기반 청산, 'TRAILING' 상태값 모두 삭제).

    Returns:
        (result, close_price, close_date, tp_hit_date,
         max_price, max_price_date, min_price, min_price_date,
         days_held, result_pct, max_achievement_pct)

        - result ∈ {'WIN', 'LOSS', 'EXPIRED', None}
        - tp_hit_date 는 WIN 일 때만 채워지고, 그 외에는 ''
        - max_achievement_pct: 최고가가 TP 대비 몇 % 도달했는지 (참고용)
    """
    max_price = entry_price
    max_price_date = ''
    min_price = entry_price
    min_price_date = ''
    tp_pct_val = (tp_price - entry_price) / entry_price if entry_price > 0 else 0

    for day_i, (dt, row) in enumerate(hist_after_entry.iterrows()):
        try:
            h = float(row['High'])
            l = float(row['Low'])
            c = float(row['Close'])
        except Exception as e:
            logging.warning(f"Failed to parse price data for {dt}: {e}")
            continue

        date_str = dt.strftime('%Y-%m-%d')
        days_held = day_i + 1

        # max/min 업데이트 (achievement 계산용 참고값)
        if h > max_price:
            max_price = h
            max_price_date = date_str
        if l < min_price:
            min_price = l
            min_price_date = date_str

        # ── [v5] 둘째 날부터: 시가가 이미 손절가/익절가를 넘어 시작한 경우 시가 체결 ──
        if day_i > 0 or gap_from_first_bar:
            try:
                o = float(row['Open'])
            except Exception:
                o = float('nan')
            if not np.isnan(o) and o > 0:
                if sl_price > 0 and o <= sl_price:
                    result_pct = (o - entry_price) / entry_price * 100
                    max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
                    return ('LOSS', o, date_str, '', max_price, max_price_date,
                            min_price, min_price_date, days_held, result_pct, min(max_ach, 999))
                if tp_price > 0 and o >= tp_price:
                    result_pct = (o - entry_price) / entry_price * 100
                    return ('WIN', o, date_str, date_str, max_price, max_price_date,
                            min_price, min_price_date, days_held, result_pct, 100.0)

        tp_hit = tp_price > 0 and h >= tp_price
        sl_hit = sl_price > 0 and l <= sl_price

        # ── 같은 날 TP·SL 동시 도달: 선후 불명 → 보수적으로 LOSS ──
        # (일봉 OHLC만으로는 장중 경로를 알 수 없으므로 유리하게 가정하지 않는다)
        if tp_hit and sl_hit:
            result_pct = (sl_price - entry_price) / entry_price * 100
            max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
            return ('LOSS', sl_price, date_str, '', max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

        # ── TP 선도달 → WIN ──
        if tp_hit:
            result_pct = (tp_price - entry_price) / entry_price * 100
            return ('WIN', tp_price, date_str, date_str, max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, 100.0)

        # ── SL 선도달 → LOSS ──
        if sl_hit:
            result_pct = (sl_price - entry_price) / entry_price * 100
            max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
            return ('LOSS', sl_price, date_str, '', max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

        # ── max_hold 경과: TP / SL 모두 미도달 → EXPIRED (실패) ──
        if days_held >= max_hold:
            result_pct = (c - entry_price) / entry_price * 100
            max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
            return ('EXPIRED', c, date_str, '', max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

    # ── 아직 결과 미확정 (price data 부족 등, 진행중) ──
    if len(hist_after_entry) > 0:
        last_c = float(hist_after_entry['Close'].iloc[-1])
        days_held = len(hist_after_entry)
        max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
        return (None, last_c, None, None, max_price, max_price_date,
                min_price, min_price_date, days_held, None, min(max_ach, 999))
    return (None, None, None, None, max_price, max_price_date,
            min_price, min_price_date, 0, None, 0)


def get_most_recent_signal_file_date():
    """
    Fix #7: Find the most recent signal_*.csv file and extract its date.
    Used instead of relying on datetime.now() which is unreliable on GitHub Actions.
    Returns date string in 'YYYY-MM-DD' format, or None if no signal files found.
    """
    try:
        signal_files = glob.glob(os.path.join(DATA_DIR, 'signal_*.csv'))
        if not signal_files:
            return None
        # Sort by filename and take the latest
        signal_files.sort()
        latest_file = signal_files[-1]
        # Extract date from filename: signal_YYYY-MM-DD.csv
        basename = os.path.basename(latest_file)
        date_str = basename.replace('signal_', '').replace('.csv', '')
        # Validate format
        try:
            pd.to_datetime(date_str, format='%Y-%m-%d')
            return date_str
        except (ValueError, TypeError):
            logging.warning(f"Could not parse signal file date: {basename}")
            return None
    except Exception as e:
        logging.warning(f"Error getting most recent signal file date: {e}")
        return None


# ─── Step 0: v5 정리 — 운영하지 않는 전략 기록 보관 + 1회 재계산 ─────────────

ARCHIVE_DIR = os.path.join(DATA_DIR, 'archive_removed_strategies')


def archive_removed_strategies():
    """open/closed/history 에서 운영하지 않는 전략(2·4~10) 행을 archive 폴더로 옮긴다 (삭제 아님)."""
    moved = 0
    for name in ('open_positions.csv', 'closed_positions.csv', 'history.csv'):
        path = os.path.join(DATA_DIR, name)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
        if df.empty or 'strategy' not in df.columns:
            continue
        df['strategy'] = df['strategy'].replace(_LEGACY_STRAT_MAP)
        keep = df['strategy'].isin(ACTIVE_STRATEGIES)
        if keep.all():
            continue
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        apath = os.path.join(ARCHIVE_DIR, name)
        old = df[~keep]
        if os.path.exists(apath):
            old = pd.concat([pd.read_csv(apath, dtype=str), old], ignore_index=True).drop_duplicates()
        old.to_csv(apath, index=False)
        df[keep].to_csv(path, index=False)
        moved += int((~keep).sum())
    if moved:
        print(f"  운영하지 않는 전략 기록 {moved}행 → {ARCHIVE_DIR}/ 로 이동")


def write_v6_marker(res):
    """재계산이 한 번 끝났다는 표시. 다시 계산하지 못한 건(상장폐지 등)은 예전 method 로 남고 화면 통계에서 빠지므로
    실패가 있어도 표시를 남긴다 — 매일 전체 재계산을 반복하지 않기 위해. 다시 하려면 --reverify."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(V6_MARKER, 'w') as f:
        f.write(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} attempted={res.get('attempted', 0)} failed={res.get('failed', 0)}\n")
    if res.get('failed'):
        print(f"  [v6] 다시 계산 못 한 {res['failed']}/{res['attempted']}건은 예전 규칙 기록으로 남김(통계 제외). "
              f"다시 시도하려면 Actions 에서 reverify 실행")


def migrate_to_v6():
    """처음 v6로 돌 때 한 번: 모든 기록을 v6 규칙(종가 진입·갭 체결·-10%/2일·비용)으로 다시 계산."""
    if os.path.exists(V6_MARKER):
        return
    print("  [v6] 최초 1회 — 전체 기록을 새 규칙으로 재계산합니다")
    res = reverify_all() or {'attempted': 0, 'failed': 0}
    write_v6_marker(res)


# ─── Step 1: Register new signals as PENDING ─────────────────────────────────

def register_new_signals():
    """
    오늘자 signal CSV에서 아직 등록 안 된 신호를 PENDING으로 추가.
    Fix #7: Try to detect signal file from directory, fall back to current UTC date.
    Fix #9: Collect rows and do one pd.concat at the end instead of concat in loop.
    """
    # Fix #7: Try to find the most recent signal file
    signal_date = get_most_recent_signal_file_date()
    if signal_date is None:
        # Fallback to current UTC date
        signal_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    signal_path = os.path.join(DATA_DIR, f'signal_{signal_date}.csv')

    if not os.path.exists(signal_path):
        print(f"  오늘자 신호 파일 없음: {signal_path}")
        return

    try:
        signals = pd.read_csv(signal_path)
    except Exception as e:
        logging.warning(f"Failed to read signal file {signal_path}: {e}")
        return

    if signals.empty:
        print("  오늘 신호 없음")
        return

    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    new_count = 0
    new_rows = []  # Fix #9: Collect rows instead of concat in loop

    for _, sig in signals.iterrows():
        strategy = str(sig.get('strategy', ''))
        ticker = str(sig.get('ticker', ''))
        date = str(sig.get('date', signal_date))
        strategy = _LEGACY_STRAT_MAP.get(strategy, strategy)
        if strategy not in ACTIVE_STRATEGIES:
            continue  # v5: 운영하지 않는 전략 신호는 등록하지 않음
        # v6: 마감 뒤 전 종목 스캔에서만 잡힌 신호는 그 가격에 살 수 없었으므로 추적하지 않음 (기록·대조용)
        if 'scan_type' in signals.columns and str(sig.get('scan_type', '')) not in ('preclose', '', 'nan'):
            continue

        # 이미 등록된 건 스킵
        already_open = not open_pos.empty and (
            (open_pos['strategy'] == strategy) &
            (open_pos['ticker'] == ticker) &
            (open_pos['signal_date'] == date)
        ).any()

        already_closed = not closed_pos.empty and (
            (closed_pos['strategy'] == strategy) &
            (closed_pos['ticker'] == ticker) &
            (closed_pos['signal_date'] == date)
        ).any()

        if already_open or already_closed:
            continue

        config = STRATEGY_CONFIG.get(strategy, {})
        tp_pct = config.get('tp_pct', 0)
        sl_pct = config.get('sl_pct', None)
        signal_price = float(sig.get('price', 0))

        # 임시 TP/SL (activate에서 entry_price 기준으로 재계산됨)
        tp_price = round(signal_price * (1 + tp_pct), 2) if signal_price > 0 else ''
        sl_price_val = round(signal_price * (1 + sl_pct), 2) if sl_pct and signal_price > 0 else ''

        new_row = {
            'strategy': strategy,
            'ticker': ticker,
            'signal_date': date,
            'signal_price': str(round(signal_price, 2)),
            'entry_date': '',
            'entry_price': '',
            'tp_price': str(tp_price),
            'sl_price': str(sl_price_val),
            'tp_pct': str(tp_pct),
            'max_hold': str(config.get('max_hold', 2)),
            'status': 'PENDING',
            'current_price': '',
            'max_price': '',
            'max_price_date': '',
            'min_price': '',
            'min_price_date': '',
            'change_pct': '',
            'achievement_pct': '',
            'days_held': '0',
            'last_updated': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'scan_type': str(sig.get('scan_type', '')) if 'scan_type' in signals.columns else '',
        }

        new_rows.append(new_row)
        new_count += 1
        print(f"    + PENDING: [{strategy}] {ticker} @ ${signal_price:.2f}")

    # Fix #9: Do one concat at the end
    if new_rows:
        open_pos = pd.concat([open_pos, pd.DataFrame(new_rows)], ignore_index=True)

    save_csv(open_pos, OPEN_PATH)
    print(f"  신규 등록: {new_count}건")


# ─── Step 2: PENDING → OPEN (신호일 종가로 진입) ─────────────────────────────

def activate_pending_positions():
    """
    PENDING 상태인 포지션의 신호일 종가를 조회하여 OPEN으로 전환.
    [수정] 동일 티커/날짜의 entry를 캐시하여 복수 전략 시 동일한 entry_price 보장.
    """
    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    if open_pos.empty:
        return

    pending = open_pos[open_pos['status'] == 'PENDING']
    if pending.empty:
        print("  대기 중인 PENDING 포지션 없음")
        return

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    updated = 0

    # [수정] 동일 티커/날짜 → 동일 entry_price 보장을 위한 캐시
    entry_cache = {}  # (ticker, signal_date) → (entry_date, entry_price)

    for idx, row in pending.iterrows():
        ticker = row['ticker']
        signal_date = row['signal_date']
        cache_key = (ticker, signal_date)

        if cache_key not in entry_cache:
            entry_cache[cache_key] = get_entry_for_signal(ticker, signal_date)

        entry_date, entry_price, signal_close = entry_cache[cache_key]

        if entry_date is None or entry_price is None or entry_price <= 0:
            print(f"    대기: {ticker} 신호일 종가 아직 확정 안 됨 (signal: {signal_date}) → 다음 실행 때 진입")
            continue

        config = STRATEGY_CONFIG.get(row['strategy'], {})
        tp_pct = config.get('tp_pct', 0)
        sl_pct = config.get('sl_pct', None)

        # [수정] TP/SL을 entry_price 기준으로 재계산
        tp_price = round(entry_price * (1 + tp_pct), 2)
        sl_price = round(entry_price * (1 + sl_pct), 2) if sl_pct else ''

        open_pos.at[idx, 'entry_date'] = entry_date
        open_pos.at[idx, 'entry_price'] = str(round(entry_price, 4))
        if signal_close:
            open_pos.at[idx, 'signal_price'] = str(round(signal_close, 4))
        open_pos.at[idx, 'method'] = METHOD_V6
        open_pos.at[idx, 'tp_price'] = str(tp_price)
        open_pos.at[idx, 'sl_price'] = str(sl_price)
        open_pos.at[idx, 'status'] = 'OPEN'
        open_pos.at[idx, 'current_price'] = str(round(entry_price, 2))
        open_pos.at[idx, 'max_price'] = str(round(entry_price, 2))
        open_pos.at[idx, 'max_price_date'] = entry_date
        open_pos.at[idx, 'min_price'] = str(round(entry_price, 2))
        open_pos.at[idx, 'min_price_date'] = entry_date
        open_pos.at[idx, 'last_updated'] = today

        updated += 1
        print(f"    OPEN: [{row['strategy']}] {ticker} entry @ ${entry_price:.2f} on {entry_date}")

    save_csv(open_pos, OPEN_PATH)
    print(f"  활성화: {updated}건 PENDING → OPEN")


# ─── Step 3: Update OPEN positions ───────────────────────────────────────────

def update_open_positions():
    """
    OPEN 포지션의 현재가를 업데이트하고 익절/손절/만기 체크.
    [수정] track_position_daywise() 사용 — 날짜순 TP/SL 동시 체크.
    Fix #1: Proper exception handling with logging.
    Fix #9: Collect closed positions in list, concat once at the end.
    """
    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    if open_pos.empty:
        print("  업데이트할 OPEN 포지션 없음")
        return

    active = open_pos[open_pos['status'] == 'OPEN']
    if active.empty:
        print("  업데이트할 OPEN 포지션 없음")
        return

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # 티커별 가격 데이터 캐시 (동일 티커 복수 전략 시 중복 조회 방지)
    hist_cache = {}
    to_close = []

    for idx, row in active.iterrows():
        ticker = row['ticker']
        entry_price = float(row['entry_price']) if row['entry_price'] else 0
        strategy = row['strategy']
        config = STRATEGY_CONFIG.get(strategy, {})

        if entry_price == 0:
            continue

        entry_date = row['entry_date']
        tp_price = float(row['tp_price']) if row['tp_price'] else 0
        sl_price_val = float(row['sl_price']) if row['sl_price'] else 0
        max_hold = safe_int(row.get('max_hold'), config.get('max_hold', 2))

        # 히스토리 가져오기 (캐시)
        cache_key = (ticker, entry_date)
        if cache_key not in hist_cache:
            hist = fetch_price_data(ticker, entry_date)
            hist_cache[cache_key] = hist

        hist = drop_unfinished_bar(hist_cache[cache_key])

        if hist is None or hist.empty:
            continue

        # [v5] 보유 중 역분할이 있으면 새로 받은 가격은 분할 보정돼 있으므로,
        # 진입가·목표가·손절가를 같은 기준(새 데이터의 진입일 시가)으로 맞춘다.
        if row.get('method', '') in (METHOD_V5, METHOD_V6):
            on_entry = hist[hist.index.normalize() == pd.to_datetime(entry_date)]
            if len(on_entry):
                o_new = float(on_entry['Open' if row.get('method', '') == METHOD_V5 else 'Close'].iloc[0])
                if o_new > 0 and abs(o_new / entry_price - 1) > 0.02:
                    print(f"    분할 보정: [{strategy}] {ticker} 진입가 {entry_price:.4f} → {o_new:.4f} (목표·손절가도 같은 비율로)")
                    entry_price = o_new
                    tp_price = round(o_new * (1 + config.get('tp_pct', 0.05)), 4)
                    sl_price_val = round(o_new * (1 + config.get('sl_pct', -0.10)), 4)
                    open_pos.at[idx, 'entry_price'] = str(round(o_new, 4))
                    open_pos.at[idx, 'tp_price'] = str(tp_price)
                    open_pos.at[idx, 'sl_price'] = str(sl_price_val)

        # [v5] 진입일(D+1) 시가에 매수 → 진입일부터 추적.
        # (예전 v4 기록이 남아 있다면 신호일 종가 매수였으므로 다음 날부터)
        entry_dt = pd.to_datetime(entry_date)
        if row.get('method', '') == METHOD_V5:
            hist_tracking = hist[hist.index.normalize() >= entry_dt]
        else:   # v6(종가 진입)·v4: 다음 거래일부터
            hist_tracking = hist[hist.index.normalize() > entry_dt]

        if hist_tracking.empty:
            continue

        # 하룻밤 실험 지표: 진입(종가) → 다음 거래일 시가
        if row.get('method', '') == METHOD_V6 and len(hist_tracking) and not str(row.get('overnight_pct', '')).strip():
            try:
                open_pos.at[idx, 'overnight_pct'] = str(round(net_of_cost((float(hist_tracking['Open'].iloc[0]) / entry_price - 1) * 100), 2))
            except Exception:
                pass
        # [v6] 분봉이 있으면 분 단위로, 없으면 일봉으로 판정
        is_v6 = row.get('method', '') == METHOD_V6
        mins = fetch_minute_data(ticker, entry_date) if is_v6 else None
        if is_v6 and minute_data_valid(mins, hist_tracking):
            result = track_position_intraday(entry_price, tp_price, sl_price_val, max_hold, mins, finished_days=list(hist_tracking.index.normalize()))
            open_pos.at[idx, 'judge'] = 'minute'
        else:
            result = track_position_daywise(entry_price, tp_price, sl_price_val, max_hold, hist_tracking, gap_from_first_bar=is_v6)
            open_pos.at[idx, 'judge'] = 'daily'

        (res_status, close_price, close_date, tp_hit_date,
         max_pr, max_pr_date, min_pr, min_pr_date,
         days_held, result_pct, max_ach) = result

        # 현재가/max/min 업데이트 (청산 안 됐더라도)
        if hist_tracking is not None and len(hist_tracking) > 0:
            try:
                current = float(hist_tracking['Close'].iloc[-1])
                open_pos.at[idx, 'current_price'] = str(round(current, 2))
                change_pct = (current - entry_price) / entry_price * 100
                open_pos.at[idx, 'change_pct'] = str(round(change_pct, 2))
            except Exception as e:
                logging.warning(f"Failed to update current price for {ticker}: {e}")

        open_pos.at[idx, 'max_price'] = str(round(max_pr, 2)) if max_pr else ''
        open_pos.at[idx, 'max_price_date'] = max_pr_date or ''
        open_pos.at[idx, 'min_price'] = str(round(min_pr, 2)) if min_pr else ''
        open_pos.at[idx, 'min_price_date'] = min_pr_date or ''
        open_pos.at[idx, 'days_held'] = str(days_held)
        open_pos.at[idx, 'achievement_pct'] = str(round(max_ach, 1)) if max_ach else ''
        open_pos.at[idx, 'last_updated'] = today

        # 청산 대상인 경우
        if res_status is not None:
            to_close.append((idx, res_status, close_price, close_date,
                           tp_hit_date or '', result_pct, max_ach))
            emoji = {'WIN': '  WIN', 'LOSS': '  LOSS', 'EXPIRED': '  EXP'}
            print(f"    {emoji.get(res_status, res_status)}: [{strategy}] {ticker} "
                  f"@ ${close_price:.2f} ({result_pct:+.1f}%) on {close_date}")

    # ── 청산 처리 ──
    # Fix #9: Collect closed rows instead of concat in loop
    closed_rows = []
    closed_indices = set()

    for (idx, reason, close_price, close_date, tp_hit_date, result_pct, max_ach) in to_close:
        if idx in closed_indices:
            continue
        closed_indices.add(idx)

        row = open_pos.loc[idx].copy()
        closed_row = row.to_dict()
        closed_row['status'] = 'CLOSED'
        closed_row['close_date'] = close_date
        closed_row['close_price'] = str(round(close_price, 2))
        is_v5 = closed_row.get('method', '') in (METHOD_V5, METHOD_V6)
        closed_row['gross_pct'] = str(round(result_pct, 2))
        closed_row['cost_pct'] = str(COST_PCT if is_v5 else 0)
        closed_row['result_pct'] = str(round(net_of_cost(result_pct) if is_v5 else result_pct, 2))
        closed_row['result_status'] = reason
        closed_row['tp_hit_date'] = tp_hit_date
        closed_row['max_achievement_pct'] = str(round(max_ach, 1))

        closed_rows.append(closed_row)

    if closed_rows:
        closed_pos = pd.concat([closed_pos, pd.DataFrame(closed_rows)], ignore_index=True)

    if closed_indices:
        open_pos = open_pos.drop(index=list(closed_indices)).reset_index(drop=True)

    save_csv(open_pos, OPEN_PATH)
    save_csv(closed_pos, CLOSED_PATH)

    print(f"  업데이트 완료: {len(active)}건 조회, {len(closed_indices)}건 청산")


# ─── Step 4: Generate summary JSON for dashboard ─────────────────────────────

def generate_tracker_summary():
    """대시보드용 요약 JSON 생성"""
    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    summary = {
        'last_tracked': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        'open_count': len(open_pos[open_pos['status'] == 'OPEN']) if not open_pos.empty else 0,
        'pending_count': len(open_pos[open_pos['status'] == 'PENDING']) if not open_pos.empty else 0,
        'closed_count': len(closed_pos) if not closed_pos.empty else 0,
    }

    if not closed_pos.empty and 'result_status' in closed_pos.columns:
        summary['win_count'] = len(closed_pos[closed_pos['result_status'] == 'WIN'])
        summary['loss_count'] = len(closed_pos[closed_pos['result_status'] == 'LOSS'])
        summary['expired_count'] = len(closed_pos[closed_pos['result_status'] == 'EXPIRED'])
        # v4: 성공/실패 이분법 집계 (WIN = 성공, LOSS+EXPIRED = 실패)
        summary['fail_count'] = summary['loss_count'] + summary['expired_count']
        # [v5] 전략별 실제 추적 성적 (비용 차감 후). 화면의 '승률' 표시는 이 값을 쓴다.
        per = {}
        cp = closed_pos.copy()
        if 'method' in cp.columns:
            cp = cp[cp['method'] == METHOD_V6]
        for strat in ACTIVE_STRATEGIES:
            sd = cp[cp['strategy'] == strat]
            r = pd.to_numeric(sd.get('result_pct', pd.Series(dtype=float)), errors='coerce').dropna()
            on = pd.to_numeric(sd.get('overnight_pct', pd.Series(dtype=float)), errors='coerce').dropna()
            per[strat] = {
                'n': int(len(r)),
                'win_rate': round(float((sd['result_status'] == 'WIN').mean() * 100), 1) if len(sd) else None,
                'mean_pct': round(float(r.mean()), 2) if len(r) else None,
                'median_pct': round(float(r.median()), 2) if len(r) else None,
                'overnight_mean_pct': round(float(on.mean()), 2) if len(on) else None,
                'cost_pct': COST_PCT,
            }
        summary['live_stats'] = per

    try:
        with open(os.path.join(DATA_DIR, 'tracker_summary.json'), 'w') as f:
            json.dump(summary, f, indent=2)
    except Exception as e:
        logging.warning(f"Failed to write tracker summary: {e}")

    print(f"  요약: OPEN={summary.get('open_count', 0)} | "
          f"WIN={summary.get('win_count', 0)} | "
          f"LOSS={summary.get('loss_count', 0)} | "
          f"EXPIRED={summary.get('expired_count', 0)}")


# ─── Step 5: Re-verify all closed positions ────────────────────────────────────

def reverify_all():
    """
    [v6] 모든 청산·진행중 내역을 재검증.
    가격 데이터를 다시 조회하고, 신호일 종가 기준으로 entry_price를 재설정한 뒤
    다음 거래일부터 분봉(없으면 일봉)으로 결과를 다시 판정. 다시 계산 못 한 건은 원래 파일에 그대로 둔다.
    Fix #1: Proper exception handling with logging.
    Fix #9: Collect rows in lists, concat once at the end.
    """
    print("=" * 70)
    print("  RE-VERIFY: 전체 청산 + 진행중 내역 재검증")
    print("=" * 70)

    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)
    open_pos = load_csv(OPEN_PATH, OPEN_COLS)

    # 모든 포지션을 합쳐서 재검증
    all_positions = []

    if not closed_pos.empty:
        for _, row in closed_pos.iterrows():
            d = row.to_dict(); d['_src'] = 'closed'
            all_positions.append(d)

    if not open_pos.empty:
        active = open_pos[open_pos['status'] == 'OPEN']
        for _, row in active.iterrows():
            d = row.to_dict(); d['_src'] = 'open'
            all_positions.append(d)

    if not all_positions:
        print("  재검증할 포지션 없음")
        return {'attempted': 0, 'failed': 0}

    print(f"  총 {len(all_positions)}건 재검증 시작...")
    failed = 0

    def keep_as_is(p):
        # 다시 계산하지 못한 건은 원래 있던 파일로 그대로 돌려놓는다
        (new_closed if p.get('_src') == 'closed' else new_open).append(p)

    # entry 캐시: 동일 티커/날짜 → 동일 entry_price 보장
    entry_cache = {}
    hist_cache = {}

    new_closed = []
    new_open = []
    changed_count = 0

    for i, pos in enumerate(all_positions):
        ticker = pos.get('ticker', '')
        signal_date = pos.get('signal_date', '')
        strategy = pos.get('strategy', '')
        old_result = pos.get('result_status', '')

        if not ticker or not signal_date or not strategy:
            continue

        config = STRATEGY_CONFIG.get(strategy, {})
        if not config:
            continue

        # 1. 진입가 재조회 (신호일 종가)
        cache_key = (ticker, signal_date)
        if cache_key not in entry_cache:
            entry_cache[cache_key] = get_entry_for_signal(ticker, signal_date)

        entry_date, entry_price, signal_close = entry_cache[cache_key]

        if entry_date is None or entry_price is None:
            print(f"    SKIP: [{strategy}] {ticker} {signal_date} — 신호일 종가 데이터 없음(상장폐지·티커 변경 등)")
            failed += 1
            keep_as_is(pos)
            continue

        # 2. TP/SL 재계산
        tp_pct = config.get('tp_pct', 0)
        sl_pct = config.get('sl_pct', None)
        tp_price = round(entry_price * (1 + tp_pct), 2)
        sl_price = round(entry_price * (1 + sl_pct), 2) if sl_pct else 0
        max_hold = config.get('max_hold', 2)

        # 3. 히스토리 재조회
        hist_key = (ticker, entry_date)
        if hist_key not in hist_cache:
            # entry_date부터 max_hold+5일 여유분까지 조회
            end_dt = pd.to_datetime(entry_date) + timedelta(days=max_hold + 10)
            hist = fetch_price_data(ticker, entry_date, end_dt.strftime('%Y-%m-%d'))
            hist_cache[hist_key] = hist

        hist = drop_unfinished_bar(hist_cache[hist_key])

        if hist is None or hist.empty:
            print(f"    SKIP: [{strategy}] {ticker} — 가격 데이터 없음")
            failed += 1
            keep_as_is(pos)
            continue

        entry_dt = pd.to_datetime(entry_date)
        # [v6] 신호일 종가 매수 → 다음 거래일부터 판정 (하룻밤 갭은 시가 체결 규칙으로 반영됨)
        hist_tracking = hist[hist.index.normalize() > entry_dt]

        if hist_tracking.empty:
            failed += 1
            keep_as_is(pos)
            continue

        if len(hist_tracking):
            try:
                pos['overnight_pct'] = str(round(net_of_cost((float(hist_tracking['Open'].iloc[0]) / entry_price - 1) * 100), 2))
            except Exception:
                pass
        # 4. [v6] 분봉으로 재판정 (Alpaca 는 과거 분봉도 제공), 못 받으면 일봉으로
        mins = fetch_minute_data(ticker, entry_date)
        if minute_data_valid(mins, hist_tracking):
            result = track_position_intraday(entry_price, tp_price, sl_price, max_hold, mins, finished_days=list(hist_tracking.index.normalize()))
            pos['judge'] = 'minute'
        else:
            result = track_position_daywise(entry_price, tp_price, sl_price, max_hold, hist_tracking, gap_from_first_bar=True)
            pos['judge'] = 'daily'

        (res_status, close_price, close_date, tp_hit_date,
         max_pr, max_pr_date, min_pr, min_pr_date,
         days_held, result_pct, max_ach) = result

        # 5. 결과 업데이트
        pos['entry_date'] = entry_date
        pos['entry_price'] = str(round(entry_price, 4))
        pos['max_hold'] = str(max_hold); pos['tp_pct'] = str(tp_pct)
        pos['method'] = METHOD_V6
        if signal_close:
            pos['signal_price'] = str(round(signal_close, 4))
        pos['tp_price'] = str(tp_price)
        pos['sl_price'] = str(sl_price) if sl_price else ''
        pos['max_price'] = str(round(max_pr, 2)) if max_pr else ''
        pos['max_price_date'] = max_pr_date or ''
        pos['min_price'] = str(round(min_pr, 2)) if min_pr else ''
        pos['min_price_date'] = min_pr_date or ''
        pos['days_held'] = str(days_held)
        pos['max_achievement_pct'] = str(round(max_ach, 1)) if max_ach else ''

        if res_status is not None:
            # 청산 확정
            pos['close_date'] = close_date
            pos['close_price'] = str(round(close_price, 4))
            pos['gross_pct'] = str(round(result_pct, 2))
            pos['cost_pct'] = str(COST_PCT)
            pos['result_pct'] = str(round(net_of_cost(result_pct), 2))
            pos['result_status'] = res_status
            pos['tp_hit_date'] = tp_hit_date or ''
            pos['status'] = 'CLOSED'

            if old_result and old_result != res_status:
                print(f"    CHANGED: [{strategy}] {ticker} {signal_date}: {old_result} → {res_status} "
                      f"(entry ${entry_price:.2f}, P&L {result_pct:+.1f}%)")
                changed_count += 1
            else:
                if (i + 1) % 20 == 0:
                    print(f"    [{i+1}/{len(all_positions)}] {res_status}: [{strategy}] {ticker}")

            new_closed.append(pos)
        else:
            # 아직 진행중
            try:
                current = float(hist_tracking['Close'].iloc[-1]) if len(hist_tracking) > 0 else entry_price
                pos['current_price'] = str(round(current, 2))
                pos['change_pct'] = str(round((current - entry_price) / entry_price * 100, 2))
            except Exception as e:
                logging.warning(f"Failed to update current price in reverify: {e}")
                pos['current_price'] = str(round(entry_price, 2))
                pos['change_pct'] = '0'

            pos['status'] = 'OPEN'
            pos['last_updated'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            for f_ in CLOSE_FIELDS:      # 예전(v4) 청산 결과가 남지 않게 비운다
                pos[f_] = ''
            pos['achievement_pct'] = str(round(max_ach, 1)) if max_ach else ''

            if old_result:
                print(f"    CHANGED: [{strategy}] {ticker} {signal_date}: {old_result} → STILL OPEN "
                      f"(entry ${entry_price:.2f})")
                changed_count += 1

            new_open.append(pos)

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i+1}/{len(all_positions)} 완료")

    # 기존 PENDING은 유지
    pending_positions = []
    if not open_pos.empty:
        pending = open_pos[open_pos['status'] == 'PENDING']
        for _, row in pending.iterrows():
            pending_positions.append(row.to_dict())

    # 저장
    for p_ in new_closed + new_open:
        p_.pop('_src', None)
    new_closed_df = pd.DataFrame(new_closed)
    if not new_closed_df.empty:
        for c in CLOSED_COLS:
            if c not in new_closed_df.columns:
                new_closed_df[c] = ''
    save_csv(new_closed_df, CLOSED_PATH)

    new_open_list = new_open + pending_positions
    new_open_df = pd.DataFrame(new_open_list) if new_open_list else pd.DataFrame(columns=OPEN_COLS)
    if not new_open_df.empty:
        for c in OPEN_COLS:
            if c not in new_open_df.columns:
                new_open_df[c] = ''
    save_csv(new_open_df, OPEN_PATH)

    # 통계 (v4: WIN / LOSS / EXPIRED 3종만. TRAILING 제거)
    total = len(new_closed) + len(new_open)
    n_win = sum(1 for p in new_closed if p.get('result_status') == 'WIN')
    n_loss = sum(1 for p in new_closed if p.get('result_status') == 'LOSS')
    n_exp = sum(1 for p in new_closed if p.get('result_status') == 'EXPIRED')
    n_open = len(new_open)

    print(f"\n  재검증 완료:")
    print(f"    총: {total}건 | WIN: {n_win} | LOSS: {n_loss} | "
          f"EXPIRED: {n_exp} | 진행중: {n_open}")
    print(f"    변경된 결과: {changed_count}건")
    # 승률 = WIN / (WIN + LOSS + EXPIRED). LOSS와 EXPIRED는 모두 실패로 집계.
    closed_n = n_win + n_loss + n_exp
    win_rate = n_win / closed_n * 100 if closed_n > 0 else 0
    print(f"    승률: {win_rate:.1f}%  (실패 = LOSS + EXPIRED = {n_loss + n_exp}건)")
    print(f"    다시 계산 못 한 건: {failed}건")
    return {'attempted': len(all_positions), 'failed': failed}


# ─── Init: Backfill from history.csv ─────────────────────────────────────────

def init_from_history():
    """
    기존 history.csv에서 과거 신호를 포지션으로 초기 등록 (최초 1회).
    Fix #1: Proper exception handling with logging.
    Fix #9: Collect rows in list, concat once at the end.
    """
    hist_path = os.path.join(DATA_DIR, 'history.csv')
    if not os.path.exists(hist_path):
        print("  history.csv 없음 — 스킵")
        return

    try:
        history = pd.read_csv(hist_path)
    except Exception as e:
        logging.warning(f"Failed to read history.csv: {e}")
        return

    if history.empty:
        print("  history.csv 비어있음 — 스킵")
        return

    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    new_count = 0
    new_rows = []  # Fix #9: Collect rows instead of concat in loop

    for _, sig in history.iterrows():
        strategy = str(sig.get('strategy', ''))
        ticker = str(sig.get('ticker', ''))
        date = str(sig.get('date', ''))

        if not strategy or not ticker or not date:
            continue
        strategy = _LEGACY_STRAT_MAP.get(strategy, strategy)
        if strategy not in ACTIVE_STRATEGIES:
            continue

        already = False
        if not open_pos.empty:
            already = ((open_pos['strategy'] == strategy) &
                      (open_pos['ticker'] == ticker) &
                      (open_pos['signal_date'] == date)).any()
        if not already and not closed_pos.empty:
            already = ((closed_pos['strategy'] == strategy) &
                      (closed_pos['ticker'] == ticker) &
                      (closed_pos['signal_date'] == date)).any()

        if already:
            continue

        config = STRATEGY_CONFIG.get(strategy, {})
        signal_price = float(sig.get('price', 0))

        new_row = {c: '' for c in OPEN_COLS}
        new_row['strategy'] = strategy
        new_row['ticker'] = ticker
        new_row['signal_date'] = date
        new_row['signal_price'] = str(round(signal_price, 2))
        new_row['tp_pct'] = str(config.get('tp_pct', 0))
        new_row['max_hold'] = str(config.get('max_hold', 2))
        new_row['status'] = 'PENDING'
        new_row['days_held'] = '0'
        new_row['last_updated'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        new_rows.append(new_row)
        new_count += 1

    # Fix #9: Do one concat at the end
    if new_rows:
        open_pos = pd.concat([open_pos, pd.DataFrame(new_rows)], ignore_index=True)

    save_csv(open_pos, OPEN_PATH)
    print(f"  초기화: {new_count}건 등록 (from history.csv)")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Position Tracker')
    parser.add_argument('--init', action='store_true', help='Initialize from history.csv')
    parser.add_argument('--reverify', action='store_true', help='Re-verify all closed positions')
    parser.add_argument('--reset', action='store_true', help='(scan.yml 호환) 전체 기록 재계산')
    parser.add_argument('--register-only', action='store_true', help='오늘 신호를 PENDING 으로 등록만 (마감 전 스캔 직후)')
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 70)
    print("  Position Tracker v6 — 종가 진입 · +5% / -10% / 2일")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    print("\n[0] 정리 (운영하지 않는 전략 기록 보관)")
    archive_removed_strategies()

    if args.register_only:
        print("\n[1] 신규 신호 등록 (마감 전)")
        register_new_signals()
        return

    if args.reverify or args.reset:
        res = reverify_all() or {'attempted': 0, 'failed': 0}
        write_v6_marker(res)
        # 재검증한 날에도 그날 신호 등록·추적은 이어서 한다

    if args.init:
        print("\n[0] 초기화: history.csv → open_positions.csv")
        init_from_history()

    migrate_to_v6()

    print("\n[1] 신규 신호 등록")
    register_new_signals()

    print("\n[2] PENDING → OPEN 전환")
    activate_pending_positions()

    print("\n[3] OPEN 포지션 업데이트")
    update_open_positions()

    print("\n[4] 요약 생성")
    generate_tracker_summary()

    elapsed = time.time() - t0
    print(f"\n  Tracker 완료: {elapsed:.0f}초")


if __name__ == '__main__':
    main()
