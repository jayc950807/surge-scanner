#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Position Tracker (v4, SIMPLIFIED)

  [v4 판정 규칙 — 단순화]
    전제: 탐지된 날짜(signal_date) = 진입 날짜(entry_date). signal_date 당일
          종가에 매수한 것으로 간주하고, 다음 거래일부터 max_hold일 동안 추적.

    성공/실패 판정은 오직 세 가지 결과만 가진다.
      1. WIN     : max_hold 이내에 TP(High >= tp_price)에 먼저 도달
      2. LOSS    : max_hold 이내에 SL(Low  <= sl_price)에 먼저 도달
                   또는 같은 날 TP/SL이 동시에 도달(일봉만으로는 선후 판별 불가
                   → 보수적으로 LOSS)
      3. EXPIRED : max_hold 경과 시까지 TP / SL 모두 미도달 (실패 카테고리)

    ※ Trailing stop 관련 로직(trailing_pct, trailing_level, peak_price 기반
       청산, 'TRAILING' 상태값)은 v4에서 전면 제거되었다.

  [이전 버전(v3) 대비 변경]
    - track_position_daywise(): trailing 인자/분기 삭제. WIN/LOSS/EXPIRED만 반환.
    - update_open_positions() / reverify_all(): trailing_pct 조회·전달 삭제.
    - generate_tracker_summary(): trailing_count 집계 삭제.
    - result_status 가능값: WIN / LOSS / EXPIRED (3종)

  Usage:
    python tracker.py                # 전체 업데이트
    python tracker.py --init         # 기존 history.csv에서 포지션 초기 생성
    python tracker.py --reverify     # 모든 청산 내역 재검증 (가격 재조회)
================================================================================
"""

import yfinance as yf
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
    STRATEGY_CONFIG, STRATEGY_NAMES, STRATEGY_WINRATE, DATA_DIR, KST,
    download_batch, extract_ticker_df
)

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
]

CLOSED_COLS = OPEN_COLS + [
    'close_date', 'close_price', 'result_pct',
    'result_status',  # WIN, LOSS, EXPIRED  (v4: TRAILING 제거)
    'tp_hit_date',
    'max_achievement_pct',
]

OPEN_PATH = os.path.join(DATA_DIR, 'open_positions.csv')
CLOSED_PATH = os.path.join(DATA_DIR, 'closed_positions.csv')


# ─── Helper Functions ─────────────────────────────────────────────────────────

def load_csv(path, cols=None):
    """Load CSV file with optional column validation."""
    if os.path.exists(path):
        df = pd.read_csv(path, dtype=str)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = ''
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
    """
    yfinance로 특정 기간 가격 데이터 조회 (timezone 자동 처리).
    Fix #1: Proper exception handling with logging.
    """
    try:
        tk = yf.Ticker(ticker)
        if end_date:
            end_dt = pd.to_datetime(end_date) + timedelta(days=1)
            df = tk.history(start=start_date, end=end_dt.strftime('%Y-%m-%d'), auto_adjust=False)
        else:
            df = tk.history(start=start_date, auto_adjust=False)
        if df is not None and len(df) > 0:
            # timezone 제거 (yfinance가 America/New_York 등 반환 → naive로 통일)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            return df
        return None
    except Exception as e:
        logging.warning(f"{ticker} price fetch failed: {e}")
        return None


def get_entry_for_signal(ticker, signal_date):
    """
    signal_date 당일 종가로 진입 — entry_date = signal_date, entry_price = 종가.
    종가 확정 후 바로 매수하는 전략이므로 D+0 종가 기준.
    동일 티커/날짜 복수 전략 시 동일한 진입가를 보장함.
    """
    try:
        hist = fetch_price_data(ticker, signal_date)
        if hist is not None and len(hist) > 0:
            # signal_date 당일 데이터 찾기
            sig_dt = pd.to_datetime(signal_date).strftime('%Y-%m-%d')
            for dt, row in hist.iterrows():
                if dt.strftime('%Y-%m-%d') == sig_dt:
                    entry_price = float(row['Close'])
                    return sig_dt, entry_price
            # 당일 데이터 못 찾으면 가장 가까운 데이터 사용
            entry_date = hist.index[0].strftime('%Y-%m-%d')
            entry_price = float(hist['Close'].iloc[0])
            return entry_date, entry_price
    except Exception as e:
        logging.warning(f"get_entry_for_signal({ticker}, {signal_date}) failed: {e}")
    return None, None


def track_position_daywise(entry_price, tp_price, sl_price, max_hold, hist_after_entry):
    """
    [v4 단순화] 진입일 다음 거래일부터 하루씩 TP/SL 도달 여부만 순차 검사.

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
            'max_hold': str(config.get('max_hold', 5)),
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
        }

        new_rows.append(new_row)
        new_count += 1
        print(f"    + PENDING: [{strategy}] {ticker} @ ${signal_price:.2f}")

    # Fix #9: Do one concat at the end
    if new_rows:
        open_pos = pd.concat([open_pos, pd.DataFrame(new_rows)], ignore_index=True)

    save_csv(open_pos, OPEN_PATH)
    print(f"  신규 등록: {new_count}건")


# ─── Step 2: PENDING → OPEN (D+1 시가로 진입) ────────────────────────────────

def activate_pending_positions():
    """
    PENDING 상태인 포지션의 D+1 시가를 조회하여 OPEN으로 전환.
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
            entry_date, entry_price = get_entry_for_signal(ticker, signal_date)
            entry_cache[cache_key] = (entry_date, entry_price)

        entry_date, entry_price = entry_cache[cache_key]

        if entry_date is None or entry_price is None or entry_price <= 0:
            print(f"    Warning: {ticker} D+1 데이터 없음 (signal: {signal_date})")
            continue

        config = STRATEGY_CONFIG.get(row['strategy'], {})
        tp_pct = config.get('tp_pct', 0)
        sl_pct = config.get('sl_pct', None)

        # [수정] TP/SL을 entry_price 기준으로 재계산
        tp_price = round(entry_price * (1 + tp_pct), 2)
        sl_price = round(entry_price * (1 + sl_pct), 2) if sl_pct else ''

        open_pos.at[idx, 'entry_date'] = entry_date
        open_pos.at[idx, 'entry_price'] = str(round(entry_price, 2))
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
        max_hold = int(row['max_hold']) if row['max_hold'] else config.get('max_hold', 5)

        # 히스토리 가져오기 (캐시)
        cache_key = (ticker, entry_date)
        if cache_key not in hist_cache:
            hist = fetch_price_data(ticker, entry_date)
            hist_cache[cache_key] = hist

        hist = hist_cache[cache_key]

        if hist is None or hist.empty:
            continue

        # 진입일 종가에 매수했으므로, 다음 거래일부터 추적
        entry_dt = pd.to_datetime(entry_date)
        hist_tracking = hist[hist.index > entry_dt]

        if hist_tracking.empty:
            continue

        # [v4] 날짜순 TP/SL 선도달 체크 (trailing 제거)
        result = track_position_daywise(
            entry_price, tp_price, sl_price_val,
            max_hold, hist_tracking
        )

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
        closed_row['close_date'] = close_date
        closed_row['close_price'] = str(round(close_price, 2))
        closed_row['result_pct'] = str(round(result_pct, 2))
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
    [신규] 모든 청산 내역을 재검증.
    가격 데이터를 다시 조회하고, D+1 시가 기준으로 entry_price를 재설정한 뒤
    날짜순 TP/SL 동시 체크로 결과를 다시 판정.
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
            all_positions.append(row.to_dict())

    if not open_pos.empty:
        active = open_pos[open_pos['status'] == 'OPEN']
        for _, row in active.iterrows():
            all_positions.append(row.to_dict())

    if not all_positions:
        print("  재검증할 포지션 없음")
        return

    print(f"  총 {len(all_positions)}건 재검증 시작...")

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

        # 1. Entry price 재조회 (D+1 시가)
        cache_key = (ticker, signal_date)
        if cache_key not in entry_cache:
            entry_date, entry_price = get_entry_for_signal(ticker, signal_date)
            if entry_date and entry_price and entry_price > 0:
                entry_cache[cache_key] = (entry_date, entry_price)
            else:
                entry_cache[cache_key] = (None, None)
            time.sleep(0.5)  # rate limiting

        entry_date, entry_price = entry_cache[cache_key]

        if entry_date is None or entry_price is None:
            print(f"    SKIP: [{strategy}] {ticker} {signal_date} — D+1 데이터 없음")
            # 데이터 없는 건은 그대로 유지
            pos['result_status'] = pos.get('result_status', 'NO_DATA')
            new_closed.append(pos)
            continue

        # 2. TP/SL 재계산
        tp_pct = config.get('tp_pct', 0)
        sl_pct = config.get('sl_pct', None)
        tp_price = round(entry_price * (1 + tp_pct), 2)
        sl_price = round(entry_price * (1 + sl_pct), 2) if sl_pct else 0
        max_hold = config.get('max_hold', 5)

        # 3. 히스토리 재조회
        hist_key = (ticker, entry_date)
        if hist_key not in hist_cache:
            # entry_date부터 max_hold+5일 여유분까지 조회
            end_dt = pd.to_datetime(entry_date) + timedelta(days=max_hold + 10)
            hist = fetch_price_data(ticker, entry_date, end_dt.strftime('%Y-%m-%d'))
            hist_cache[hist_key] = hist
            time.sleep(0.5)

        hist = hist_cache[hist_key]

        if hist is None or hist.empty:
            print(f"    SKIP: [{strategy}] {ticker} — 가격 데이터 없음")
            pos['result_status'] = pos.get('result_status', 'NO_DATA')
            new_closed.append(pos)
            continue

        entry_dt = pd.to_datetime(entry_date)
        # [v4 버그수정] 진입일 자체는 추적에서 제외 (update_open_positions 와 일치).
        # 진입가는 당일 종가이므로 당일 장중 고/저가를 TP/SL 판정이나 peak 기록에
        # 사용하면 lookahead bias 가 발생한다 (매수 이전 시점 가격을 후행 판정에 씀).
        hist_tracking = hist[hist.index > entry_dt]

        if hist_tracking.empty:
            pos['result_status'] = 'NO_DATA'
            new_closed.append(pos)
            continue

        # 4. [v4] 날짜순 TP/SL 선도달 체크로 재판정 (trailing 제거)
        result = track_position_daywise(
            entry_price, tp_price, sl_price,
            max_hold, hist_tracking
        )

        (res_status, close_price, close_date, tp_hit_date,
         max_pr, max_pr_date, min_pr, min_pr_date,
         days_held, result_pct, max_ach) = result

        # 5. 결과 업데이트
        pos['entry_date'] = entry_date
        pos['entry_price'] = str(round(entry_price, 2))
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
            pos['close_price'] = str(round(close_price, 2))
            pos['result_pct'] = str(round(result_pct, 2))
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

            if old_result:
                print(f"    CHANGED: [{strategy}] {ticker} {signal_date}: {old_result} → STILL OPEN "
                      f"(entry ${entry_price:.2f})")
                changed_count += 1

            new_open.append(pos)

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i+1}/{len(all_positions)} 완료")
            time.sleep(1)  # rate limiting

    # 기존 PENDING은 유지
    pending_positions = []
    if not open_pos.empty:
        pending = open_pos[open_pos['status'] == 'PENDING']
        for _, row in pending.iterrows():
            pending_positions.append(row.to_dict())

    # 저장
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
        new_row['max_hold'] = str(config.get('max_hold', 5))
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
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 70)
    print("  Position Tracker v3 (Fixed) — Update")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if args.reverify:
        reverify_all()
        print("\n[+] 요약 업데이트")
        generate_tracker_summary()
        elapsed = time.time() - t0
        print(f"\n  완료: {elapsed:.0f}초")
        return

    if args.init:
        print("\n[0] 초기화: history.csv → open_positions.csv")
        init_from_history()

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
