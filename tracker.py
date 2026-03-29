#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Position Tracker

  신호 발생 후 포지션을 추적하고, 익절/손절/만기 여부를 자동 업데이트합니다.

  기능:
    1. 새 신호 → open_positions.csv에 PENDING으로 등록
    2. PENDING → 신호가(종가)로 즉시 OPEN 전환 (애프터마켓 매수)
    3. OPEN → 매일 현재가 조회하여 max_price, current_price, achievement_pct 업데이트
    4. 익절/손절/만기 도달 시 → closed_positions.csv로 이동

  Usage:
    python tracker.py                # 전체 업데이트
    python tracker.py --init         # 기존 history.csv에서 포지션 초기 생성
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
    'result_status',  # WIN, LOSS, EXPIRED
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
    """두 날짜 사이의 거래일 수 (주말 제외, 공휴일은 미포함)"""
    days = pd.bdate_range(start=start_date, end=end_date)
    return len(days) - 1  # start_date 제외


def fetch_price_data(ticker, start_date, end_date=None):
    """yfinance로 특정 기간 가격 데이터 조회"""
    try:
        tk = yf.Ticker(ticker)
        if end_date:
            # end_date는 exclusive이므로 +1일
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


# ─── Step 1: Register new signals as PENDING ─────────────────────────────────

def register_new_signals():
    """data/ 폴더의 모든 signal CSV에서 아직 등록 안 된 신호를 PENDING으로 추가"""
    import glob
    signal_files = sorted(glob.glob(os.path.join(DATA_DIR, 'signal_*.csv')))

    if not signal_files:
        print("  신호 파일 없음")
        return

    # 모든 signal 파일을 합침
    all_signals = []
    for sf in signal_files:
        try:
            df = pd.read_csv(sf)
            if not df.empty:
                all_signals.append(df)
        except Exception:
            continue

    if not all_signals:
        print("  등록할 신호 없음")
        return

    signals = pd.concat(all_signals, ignore_index=True)
    print(f"  signal 파일 {len(signal_files)}개에서 총 {len(signals)}건 로드")

    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    closed_pos = load_csv(CLOSED_PATH, CLOSED_COLS)

    new_count = 0
    for _, sig in signals.iterrows():
        strategy = str(sig.get('strategy', ''))
        ticker = str(sig.get('ticker', ''))
        date = str(sig.get('date', ''))

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


# ─── Step 2: PENDING → OPEN (신호가=종가로 즉시 진입, 애프터마켓 매수) ────────

def activate_pending_positions():
    """PENDING → 신호가(종가)로 즉시 OPEN 전환 (애프터마켓 매수 기준)"""
    open_pos = load_csv(OPEN_PATH, OPEN_COLS)
    if open_pos.empty:
        return

    pending = open_pos[open_pos['status'] == 'PENDING']
    if pending.empty:
        print("  대기 중인 PENDING 포지션 없음")
        return

    today = datetime.now().strftime('%Y-%m-%d')
    updated = 0

    for idx, row in pending.iterrows():
        ticker = row['ticker']
        signal_date = row['signal_date']
        signal_price = float(row['signal_price']) if row['signal_price'] else 0

        if signal_price <= 0:
            print(f"    ⚠ {ticker} signal_price 없음 — 스킵")
            continue

        # 매수가 = 신호가(종가) — 애프터마켓에서 종가 근처로 매수
        entry_price = signal_price
        entry_date = signal_date

        config = STRATEGY_CONFIG.get(row['strategy'], {})
        tp_pct = config.get('tp_pct', 0)
        sl_pct = config.get('sl_pct', None)

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
        print(f"    ✓ OPEN: [{row['strategy']}] {ticker} entry @ ${entry_price:.2f} on {entry_date} (신호가 매수)")

    save_csv(open_pos, OPEN_PATH)
    print(f"  활성화: {updated}건 PENDING → OPEN")


# ─── Step 3: Update OPEN positions ───────────────────────────────────────────

def update_open_positions():
    """OPEN 포지션의 현재가를 업데이트하고 익절/손절/만기 체크"""
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
    tickers = active['ticker'].unique().tolist()

    print(f"  현재가 조회: {len(tickers)}개 티커")
    current_prices = fetch_current_prices(tickers)

    to_close = []  # (index, close_reason, close_price, close_date)

    for idx, row in active.iterrows():
        ticker = row['ticker']
        entry_price = float(row['entry_price']) if row['entry_price'] else 0
        strategy = row['strategy']
        config = STRATEGY_CONFIG.get(strategy, {})

        if entry_price == 0:
            continue

        # 진입일부터의 상세 가격 데이터 조회 (일중 고/저 체크를 위해)
        entry_date = row['entry_date']
        hist = fetch_price_data(ticker, entry_date)

        if hist is not None and len(hist) > 0:
            # 진입일 이후의 데이터만 (진입일 포함)
            max_high = float(hist['High'].max())
            min_low = float(hist['Low'].min())
            max_high_date = hist['High'].idxmax().strftime('%Y-%m-%d')
            min_low_date = hist['Low'].idxmin().strftime('%Y-%m-%d')
            current = float(hist['Close'].iloc[-1])
            current_date = hist.index[-1].strftime('%Y-%m-%d')

            # 기존 max/min과 비교
            prev_max = float(row['max_price']) if row['max_price'] else 0
            prev_min = float(row['min_price']) if row['min_price'] else float('inf')

            if max_high > prev_max:
                open_pos.at[idx, 'max_price'] = str(round(max_high, 2))
                open_pos.at[idx, 'max_price_date'] = max_high_date

            if min_low < prev_min:
                open_pos.at[idx, 'min_price'] = str(round(min_low, 2))
                open_pos.at[idx, 'min_price_date'] = min_low_date

            final_max = max(max_high, prev_max)
            open_pos.at[idx, 'current_price'] = str(round(current, 2))

            # 수익률
            change_pct = (current - entry_price) / entry_price * 100
            open_pos.at[idx, 'change_pct'] = str(round(change_pct, 2))

            # 목표 달성률 (max_price 기준)
            tp_pct = config.get('tp_pct', 0)
            if tp_pct > 0:
                max_gain = (final_max - entry_price) / entry_price
                achievement = min(max_gain / tp_pct * 100, 100)
                open_pos.at[idx, 'achievement_pct'] = str(round(achievement, 1))

            # 보유일수
            days_held = get_trading_days_between(entry_date, today)
            open_pos.at[idx, 'days_held'] = str(days_held)
            open_pos.at[idx, 'last_updated'] = today

            # ── 익절 체크: 진입일 다음날부터 일중 고가가 TP에 도달했는지 ──
            tp_price = float(row['tp_price']) if row['tp_price'] else 0
            if tp_price > 0:
                # 날짜별로 TP 도달 확인 (진입일 제외 — 종가 매수이므로)
                for hist_date, hist_row in hist.iterrows():
                    if hist_date.strftime('%Y-%m-%d') == entry_date:
                        continue  # 진입일(종가 매수일)은 제외
                    if float(hist_row['High']) >= tp_price:
                        tp_hit_date = hist_date.strftime('%Y-%m-%d')
                        to_close.append((idx, 'WIN', tp_price, tp_hit_date, tp_hit_date))
                        print(f"    ✅ WIN: [{strategy}] {ticker} TP ${tp_price:.2f} hit on {tp_hit_date}")
                        break
                else:
                    # ── 손절 체크 ──
                    sl_price = float(row['sl_price']) if row['sl_price'] else 0
                    if sl_price > 0:
                        for hist_date, hist_row in hist.iterrows():
                            if hist_date.strftime('%Y-%m-%d') == entry_date:
                                continue  # 진입일은 제외
                            if float(hist_row['Low']) <= sl_price:
                                sl_hit_date = hist_date.strftime('%Y-%m-%d')
                                to_close.append((idx, 'LOSS', sl_price, sl_hit_date, ''))
                                print(f"    ❌ LOSS: [{strategy}] {ticker} SL ${sl_price:.2f} hit on {sl_hit_date}")
                                break
                        else:
                            # ── 만기 체크 ──
                            max_hold = int(row['max_hold']) if row['max_hold'] else 5
                            if days_held >= max_hold:
                                to_close.append((idx, 'EXPIRED', current, current_date, ''))
                                print(f"    ⏰ EXPIRED: [{strategy}] {ticker} {days_held}d held, close @ ${current:.2f}")
                    else:
                        # 손절 없는 전략 (D, E)
                        max_hold = int(row['max_hold']) if row['max_hold'] else 30
                        if days_held >= max_hold:
                            to_close.append((idx, 'EXPIRED', current, current_date, ''))
                            print(f"    ⏰ EXPIRED: [{strategy}] {ticker} {days_held}d held, close @ ${current:.2f}")

            # Strategy A 트레일링 스탑 체크
            if strategy == 'A' and config.get('trailing_pct'):
                trailing_pct = config['trailing_pct']
                if final_max > entry_price:  # 수익 구간에서만
                    trailing_level = final_max * (1 + trailing_pct)
                    if current <= trailing_level:
                        # 이미 tp_hit이나 sl_hit으로 닫히지 않았다면
                        if not any(c[0] == idx for c in to_close):
                            to_close.append((idx, 'WIN', current, current_date, ''))
                            print(f"    🔄 TRAILING: [{strategy}] {ticker} trailing stop @ ${current:.2f}")

        else:
            # 가격 데이터 없음 - 현재가만 업데이트 시도
            if ticker in current_prices:
                cp = current_prices[ticker]
                open_pos.at[idx, 'current_price'] = str(round(cp['close'], 2))
                open_pos.at[idx, 'last_updated'] = today

    # ── 청산 처리 ──
    closed_indices = set()
    for (idx, reason, close_price, close_date, tp_hit_date) in to_close:
        if idx in closed_indices:
            continue
        closed_indices.add(idx)

        row = open_pos.loc[idx].copy()
        entry_price = float(row['entry_price']) if row['entry_price'] else 0
        result_pct = ((close_price - entry_price) / entry_price * 100) if entry_price > 0 else 0

        config = STRATEGY_CONFIG.get(row['strategy'], {})
        tp_pct = config.get('tp_pct', 0)
        max_price = float(row['max_price']) if row['max_price'] else close_price
        max_achievement = min((max_price - entry_price) / entry_price / tp_pct * 100, 999) if tp_pct > 0 and entry_price > 0 else 0

        closed_row = row.to_dict()
        closed_row['close_date'] = close_date
        closed_row['close_price'] = str(round(close_price, 2))
        closed_row['result_pct'] = str(round(result_pct, 2))
        closed_row['result_status'] = reason
        closed_row['tp_hit_date'] = tp_hit_date if tp_hit_date else ''
        closed_row['max_achievement_pct'] = str(round(max_achievement, 1))

        closed_pos = pd.concat([closed_pos, pd.DataFrame([closed_row])], ignore_index=True)

    # OPEN 목록에서 청산된 것 제거
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

    with open(os.path.join(DATA_DIR, 'tracker_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"  요약: OPEN={summary.get('open_count', 0)} | "
          f"WIN={summary.get('win_count', 0)} | "
          f"LOSS={summary.get('loss_count', 0)} | "
          f"EXPIRED={summary.get('expired_count', 0)}")


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

        # 이미 있는지 체크
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
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 70)
    print("  Position Tracker — Update")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

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
