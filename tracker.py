#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Position Tracker (FIXED v2)

  수정 사항:
    1. TP/SL 체크를 날짜순으로 하루씩 동시 확인 (기존: TP 전체→SL 전체 순서 버그)
    2. 동일 티커/날짜 복수 전략 시 entry_price 일관성 보장
    3. 진입일(entry_date) 포함 TP/SL 체크 통일
    4. --reverify 옵션: 전체 청산 내역 재검증
    5. trailing stop을 날짜순 루프 안에서 처리

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
from datetime import datetime, timedelta

# ─── Configuration ────────────────────────────────────────────────────────────

STRATEGY_CONFIG = {
    'A': {'tp_pct': 0.05, 'sl_pct': -0.20, 'trailing_pct': -0.03, 'max_hold': 5},
    'B': {'tp_pct': 0.15, 'sl_pct': -0.20, 'trailing_pct': None,  'max_hold': 10},
    'C': {'tp_pct': 0.05, 'sl_pct': -0.20, 'trailing_pct': None,  'max_hold': 5},
    'D': {'tp_pct': 0.20, 'sl_pct': None,   'trailing_pct': None,  'max_hold': 30},
    'E': {'tp_pct': 0.10, 'sl_pct': None,   'trailing_pct': None,  'max_hold': 30},
}

STRATEGY_NAMES = {
    'A': '급락 반등 +5%',
    'B': '고수익 +15%',
    'C': '과매도 반등 +5%',
    'D': '초저가 폭락 +20%',
    'E': '급락 속반등 +10%',
}

DATA_DIR = 'data'
OPEN_PATH = os.path.join(DATA_DIR, 'open_positions.csv')
CLOSED_PATH = os.path.join(DATA_DIR, 'closed_positions.csv')

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
    'result_status',  # WIN, LOSS, EXPIRED, TRAILING
    'tp_hit_date',
    'max_achievement_pct',
]


# ─── Helper Functions ─────────────────────────────────────────────────────────

def load_csv(path, cols=None):
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
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(path, index=False)


def get_trading_days_between(start_date, end_date):
    """두 날짜 사이의 거래일 수 (주말 제외)"""
    days = pd.bdate_range(start=start_date, end=end_date)
    return len(days) - 1  # start_date 제외


def fetch_price_data(ticker, start_date, end_date=None):
    """yfinance로 특정 기간 가격 데이터 조회"""
    try:
        tk = yf.Ticker(ticker)
        if end_date:
            end_dt = pd.to_datetime(end_date) + timedelta(days=1)
            df = tk.history(start=start_date, end=end_dt.strftime('%Y-%m-%d'))
        else:
            df = tk.history(start=start_date)
        return df if len(df) > 0 else None
    except Exception as e:
        print(f"    Warning: {ticker} price fetch failed: {e}")
        return None


def fetch_current_prices(tickers):
    """여러 티커의 현재가를 배치로 조회"""
    if not tickers:
        return {}

    prices = {}
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            data = yf.download(' '.join(batch), period='5d',
                             group_by='ticker', progress=False, threads=True, timeout=30)
            if data is None or data.empty:
                continue

            for tk in batch:
                try:
                    if len(batch) == 1:
                        if isinstance(data.columns, pd.MultiIndex):
                            df = data.xs(tk, level=1, axis=1) if tk in data.columns.get_level_values(1) else data
                        else:
                            df = data
                    else:
                        if isinstance(data.columns, pd.MultiIndex):
                            level_values = [set(data.columns.get_level_values(i)) for i in range(data.columns.nlevels)]
                            ticker_level = None
                            for lvl_i, vals in enumerate(level_values):
                                if tk in vals:
                                    ticker_level = lvl_i
                                    break
                            if ticker_level is not None:
                                df = data.xs(tk, level=ticker_level, axis=1)
                            else:
                                continue
                        else:
                            continue

                    df = df.dropna(how='all')
                    if len(df) > 0:
                        prices[tk] = {
                            'close': float(df['Close'].iloc[-1]),
                            'high': float(df['High'].iloc[-1]),
                            'low': float(df['Low'].iloc[-1]),
                            'date': df.index[-1].strftime('%Y-%m-%d'),
                        }
                except:
                    continue
        except:
            continue
        time.sleep(1)

    return prices


def get_entry_for_signal(ticker, signal_date):
    """
    signal_date 다음 거래일의 시가를 조회하여 entry_date와 entry_price를 반환.
    동일 티커/날짜 복수 전략 시 동일한 진입가를 보장함.
    """
    try:
        next_day = pd.to_datetime(signal_date) + timedelta(days=1)
        hist = fetch_price_data(ticker, next_day.strftime('%Y-%m-%d'))
        if hist is not None and len(hist) > 0:
            entry_date = hist.index[0].strftime('%Y-%m-%d')
            entry_price = float(hist['Open'].iloc[0])
            return entry_date, entry_price
    except:
        pass
    return None, None


def track_position_daywise(entry_price, tp_price, sl_price, trailing_pct, max_hold, hist_after_entry):
    """
    [핵심 수정] 날짜순으로 하루씩 TP/SL/Trailing을 동시 체크.
    기존 버그: TP를 전체 기간에서 먼저 찾고, 없으면 SL을 전체 기간에서 찾음
    → SL이 먼저 맞았는데 나중에 TP 맞은 경우 WIN으로 잘못 처리됨

    Returns: (result, close_price, close_date, tp_hit_date, max_price, max_price_date,
              min_price, min_price_date, days_held, result_pct, max_achievement_pct)
    """
    peak_price = entry_price
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
        except:
            continue

        date_str = dt.strftime('%Y-%m-%d')
        days_held = day_i + 1

        # max/min 업데이트
        if h > max_price:
            max_price = h
            max_price_date = date_str
        if l < min_price:
            min_price = l
            min_price_date = date_str

        # peak 업데이트 (trailing용)
        if h > peak_price:
            peak_price = h

        # ── 같은 날 TP와 SL 동시 체크 ──
        tp_hit = tp_price > 0 and h >= tp_price
        sl_hit = sl_price > 0 and l <= sl_price

        if tp_hit and sl_hit:
            # 같은 날 둘 다 가능한 경우:
            # 시가에서 TP/SL 중 어느 쪽이 가까운지로 판단
            o = float(row['Open'])
            dist_to_tp = abs(tp_price - o)
            dist_to_sl = abs(sl_price - o)
            if dist_to_tp <= dist_to_sl:
                # TP가 시가에 더 가까움 → TP 먼저 도달 가능성 높음
                result_pct = (tp_price - entry_price) / entry_price * 100
                max_ach = 100.0
                return ('WIN', tp_price, date_str, date_str, max_price, max_price_date,
                        min_price, min_price_date, days_held, result_pct, max_ach)
            else:
                # SL이 시가에 더 가까움 → SL 먼저 도달
                result_pct = (sl_price - entry_price) / entry_price * 100
                max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
                return ('LOSS', sl_price, date_str, '', max_price, max_price_date,
                        min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

        if tp_hit:
            result_pct = (tp_price - entry_price) / entry_price * 100
            return ('WIN', tp_price, date_str, date_str, max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, 100.0)

        if sl_hit:
            result_pct = (sl_price - entry_price) / entry_price * 100
            max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
            return ('LOSS', sl_price, date_str, '', max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

        # ── Trailing stop 체크 ──
        if trailing_pct and peak_price > entry_price:
            trailing_level = peak_price * (1 + trailing_pct)
            if c <= trailing_level:
                result_pct = (c - entry_price) / entry_price * 100
                max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
                return ('TRAILING', c, date_str, '', max_price, max_price_date,
                        min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

        # ── 만기 체크 ──
        if days_held >= max_hold:
            result_pct = (c - entry_price) / entry_price * 100
            max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
            return ('EXPIRED', c, date_str, '', max_price, max_price_date,
                    min_price, min_price_date, days_held, result_pct, min(max_ach, 999))

    # 아직 결과 미확정 (진행중)
    if len(hist_after_entry) > 0:
        last_c = float(hist_after_entry['Close'].iloc[-1])
        days_held = len(hist_after_entry)
        max_ach = (max_price - entry_price) / entry_price / tp_pct_val * 100 if tp_pct_val > 0 else 0
        return (None, last_c, None, None, max_price, max_price_date,
                min_price, min_price_date, days_held, None, min(max_ach, 999))
    return (None, None, None, None, max_price, max_price_date,
            min_price, min_price_date, 0, None, 0)


# ─── Step 1: Register new signals as PENDING ─────────────────────────────────

def register_new_signals():
    """오늘자 signal CSV에서 아직 등록 안 된 신호를 PENDING으로 추가"""
    today = datetime.now().strftime('%Y-%m-%d')
    signal_path = os.path.join(DATA_DIR, f'signal_{today}.csv')

    if not os.path.exists(signal_path):
        print(f"  오늘자 신호 파일 없음: {signal_path}")
        return

    signals = pd.read_csv(signal_path)
    if signals.empty:
        print("  오늘 신호 없음")
        return

    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    new_count = 0
    for _, sig in signals.iterrows():
        strategy = str(sig.get('strategy', ''))
        ticker = str(sig.get('ticker', ''))
        date = str(sig.get('date', today))

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
            'last_updated': today,
        }

        open_pos = pd.concat([open_pos, pd.DataFrame([new_row])], ignore_index=True)
        new_count += 1
        print(f"    + PENDING: [{strategy}] {ticker} @ ${signal_price:.2f}")

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

    today = datetime.now().strftime('%Y-%m-%d')
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

    today = datetime.now().strftime('%Y-%m-%d')

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
        trailing_pct = config.get('trailing_pct')
        max_hold = int(row['max_hold']) if row['max_hold'] else config.get('max_hold', 5)

        # 히스토리 가져오기 (캐시)
        cache_key = (ticker, entry_date)
        if cache_key not in hist_cache:
            hist = fetch_price_data(ticker, entry_date)
            hist_cache[cache_key] = hist

        hist = hist_cache[cache_key]

        if hist is None or hist.empty:
            continue

        # 진입일 이후의 데이터만 (진입일 다음날부터 추적)
        entry_dt = pd.to_datetime(entry_date)
        # 진입일 시가에 매수했으므로, 진입일의 나머지 시간도 추적 대상
        hist_tracking = hist[hist.index >= entry_dt]

        if hist_tracking.empty:
            continue

        # [핵심 수정] 날짜순 TP/SL 동시 체크
        result = track_position_daywise(
            entry_price, tp_price, sl_price_val,
            trailing_pct, max_hold, hist_tracking
        )

        (res_status, close_price, close_date, tp_hit_date,
         max_pr, max_pr_date, min_pr, min_pr_date,
         days_held, result_pct, max_ach) = result

        # 현재가/max/min 업데이트 (청산 안 됐더라도)
        if hist_tracking is not None and len(hist_tracking) > 0:
            current = float(hist_tracking['Close'].iloc[-1])
            open_pos.at[idx, 'current_price'] = str(round(current, 2))
            change_pct = (current - entry_price) / entry_price * 100
            open_pos.at[idx, 'change_pct'] = str(round(change_pct, 2))

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
            emoji = {'WIN': '  WIN', 'LOSS': '  LOSS', 'TRAILING': '  TRAIL', 'EXPIRED': '  EXP'}
            print(f"    {emoji.get(res_status, res_status)}: [{strategy}] {ticker} "
                  f"@ ${close_price:.2f} ({result_pct:+.1f}%) on {close_date}")

    # ── 청산 처리 ──
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

        closed_pos = pd.concat([closed_pos, pd.DataFrame([closed_row])], ignore_index=True)

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
        'last_tracked': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'open_count': len(open_pos[open_pos['status'] == 'OPEN']) if not open_pos.empty else 0,
        'pending_count': len(open_pos[open_pos['status'] == 'PENDING']) if not open_pos.empty else 0,
        'closed_count': len(closed_pos) if not closed_pos.empty else 0,
    }

    if not closed_pos.empty and 'result_status' in closed_pos.columns:
        summary['win_count'] = len(closed_pos[closed_pos['result_status'] == 'WIN'])
        summary['loss_count'] = len(closed_pos[closed_pos['result_status'] == 'LOSS'])
        summary['expired_count'] = len(closed_pos[closed_pos['result_status'] == 'EXPIRED'])
        summary['trailing_count'] = len(closed_pos[closed_pos['result_status'] == 'TRAILING'])

    with open(os.path.join(DATA_DIR, 'tracker_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"  요약: OPEN={summary.get('open_count', 0)} | "
          f"WIN={summary.get('win_count', 0)} | "
          f"LOSS={summary.get('loss_count', 0)} | "
          f"EXPIRED={summary.get('expired_count', 0)} | "
          f"TRAILING={summary.get('trailing_count', 0)}")


# ─── Step 5: Re-verify all closed positions ────────────────────────────────────

def reverify_all():
    """
    [신규] 모든 청산 내역을 재검증.
    가격 데이터를 다시 조회하고, D+1 시가 기준으로 entry_price를 재설정한 뒤
    날짜순 TP/SL 동시 체크로 결과를 다시 판정.
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
        trailing_pct = config.get('trailing_pct')
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
        hist_tracking = hist[hist.index >= entry_dt]

        if hist_tracking.empty:
            pos['result_status'] = 'NO_DATA'
            new_closed.append(pos)
            continue

        # 4. 날짜순 TP/SL 동시 체크로 재판정
        result = track_position_daywise(
            entry_price, tp_price, sl_price,
            trailing_pct, max_hold, hist_tracking
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
            current = float(hist_tracking['Close'].iloc[-1]) if len(hist_tracking) > 0 else entry_price
            pos['current_price'] = str(round(current, 2))
            pos['change_pct'] = str(round((current - entry_price) / entry_price * 100, 2))
            pos['status'] = 'OPEN'
            pos['last_updated'] = datetime.now().strftime('%Y-%m-%d')

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

    # 통계
    total = len(new_closed) + len(new_open)
    n_win = sum(1 for p in new_closed if p.get('result_status') == 'WIN')
    n_loss = sum(1 for p in new_closed if p.get('result_status') == 'LOSS')
    n_trail = sum(1 for p in new_closed if p.get('result_status') == 'TRAILING')
    n_exp = sum(1 for p in new_closed if p.get('result_status') == 'EXPIRED')
    n_open = len(new_open)

    print(f"\n  재검증 완료:")
    print(f"    총: {total}건 | WIN: {n_win} | LOSS: {n_loss} | "
          f"TRAILING: {n_trail} | EXPIRED: {n_exp} | 진행중: {n_open}")
    print(f"    변경된 결과: {changed_count}건")
    win_rate = n_win / (n_win + n_loss + n_trail + n_exp) * 100 if (n_win + n_loss + n_trail + n_exp) > 0 else 0
    print(f"    승률: {win_rate:.1f}%")


# ─── Init: Backfill from history.csv ─────────────────────────────────────────

def init_from_history():
    """기존 history.csv에서 과거 신호를 포지션으로 초기 등록 (최초 1회)"""
    hist_path = os.path.join(DATA_DIR, 'history.csv')
    if not os.path.exists(hist_path):
        print("  history.csv 없음 — 스킵")
        return

    history = pd.read_csv(hist_path)
    if history.empty:
        print("  history.csv 비어있음 — 스킵")
        return

    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    new_count = 0
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
        new_row['last_updated'] = datetime.now().strftime('%Y-%m-%d')

        open_pos = pd.concat([open_pos, pd.DataFrame([new_row])], ignore_index=True)
        new_count += 1

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
    print("  Position Tracker v2 (Fixed) — Update")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
