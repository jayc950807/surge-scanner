#!/usr/bin/env python3
"""
================================================================================
  Strategy Explorer v1 — Full-Spectrum Strategy Discovery

  6가지 전략 유형 × 파라미터 그리드 → 자동 탐색
  ────────────────────────────────────────────────────
  1. RSI_OVERSOLD      과매도 반등 (Mean Reversion)
  2. PENNY_CRASH       저가주 폭락 반등 (Mean Reversion)
  3. VOLUME_SPIKE      거래량 폭증 + 과매도 (Mean Reversion)
  4. GAP_DOWN_RECOVERY 갭다운 후 반등 (Event-based)
  5. BB_SQUEEZE        볼린저밴드 하단 이탈 → 평균회귀 (Volatility)
  6. MOMENTUM_BREAKOUT 52주 고점 돌파 + 거래량 (Momentum)
  ────────────────────────────────────────────────────
  기간: 5년 | 최소 승률: 90% | 최소 신호: 30건
  GitHub Actions 6시간 타임아웃 대응
================================================================================
"""

import pandas as pd
import numpy as np
import time
import warnings
import sys
import json
import os
import logging
from datetime import datetime

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
from shared_config import (
    calc_rsi_wilder,
    get_all_tickers,
    download_batch,
    extract_ticker_df,
)

# ─── Config ──────────────────────────────────────────────────────────────────
MIN_SIGNALS = 30
MIN_WIN_RATE = 90.0
MAX_RUNTIME_MINUTES = 330   # 5.5시간
DATA_PERIOD = '5y'


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 1: INDICATORS
# ═══════════════════════════════════════════════════════════════════════════════

def consecutive_down_days(close):
    is_down = close < close.shift(1)
    groups = (~is_down).cumsum()
    return is_down.groupby(groups).cumsum().astype(int)


def precompute_indicators(df):
    """모든 전략 유형에 필요한 지표를 한 번에 계산"""
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    opn = df['Open'].astype(float)
    vol = df['Volume'].astype(float)

    ind = {
        'close': close, 'high': high, 'low': low, 'open': opn, 'vol': vol,
        # RSI
        'rsi7': calc_rsi_wilder(close, 7),
        'rsi14': calc_rsi_wilder(close, 14),
        # Returns
        'ret_1d': close.pct_change(1) * 100,
        'ret_3d': close.pct_change(3) * 100,
        'ret_5d': close.pct_change(5) * 100,
        'ret_10d': close.pct_change(10) * 100,
        'ret_20d': close.pct_change(20) * 100,
        # Intraday range
        'intra': (high - low) / low.replace(0, np.nan) * 100,
        # Consecutive days
        'down_days': consecutive_down_days(close),
        'price': close,
    }

    # Distance from lows/highs
    ind['dist_low5'] = (close - low.rolling(5).min()) / low.rolling(5).min().replace(0, np.nan) * 100
    high_52w = high.rolling(252).max()
    ind['dist_high52w'] = (close - high_52w) / high_52w.replace(0, np.nan) * 100

    # Volume
    ind['vol_ma5'] = vol.rolling(5).mean()
    ind['vol_ma20'] = vol.rolling(20).mean()
    ind['vol_ratio'] = vol / vol.rolling(20).mean().replace(0, np.nan)

    # Moving averages
    ind['ma20'] = close.rolling(20).mean()
    ind['ma50'] = close.rolling(50).mean()
    ind['ma_pos20'] = (close - ind['ma20']) / ind['ma20'].replace(0, np.nan) * 100

    # Bollinger Bands (20, 2)
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    ind['bb_upper'] = bb_mid + 2 * bb_std
    ind['bb_lower'] = bb_mid - 2 * bb_std
    ind['bb_width'] = (ind['bb_upper'] - ind['bb_lower']) / bb_mid.replace(0, np.nan) * 100
    ind['bb_pos'] = (close - ind['bb_lower']) / (ind['bb_upper'] - ind['bb_lower']).replace(0, np.nan)

    # ATR
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    ind['atr14'] = tr.rolling(14).mean()
    ind['atr_ratio'] = tr.rolling(5).mean() / tr.rolling(20).mean().replace(0, np.nan)

    # Gap (open vs prev close)
    ind['gap_pct'] = (opn - close.shift(1)) / close.shift(1).replace(0, np.nan) * 100

    # Consecutive up days
    is_up = close > close.shift(1)
    up_groups = (~is_up).cumsum()
    ind['up_days'] = is_up.groupby(up_groups).cumsum().astype(int)

    return ind


def ticker_has_potential(ind):
    """어떤 전략이든 신호를 낼 가능성이 있는 종목인지 빠르게 판별"""
    rsi7 = ind['rsi7'].values
    price = ind['price'].values
    ret5d = ind['ret_5d'].values
    intra = ind['intra'].values
    vol_ratio = ind['vol_ratio'].values
    dh52 = ind['dist_high52w'].values

    return (np.any(rsi7[~np.isnan(rsi7)] < 35)
            or np.any(price[~np.isnan(price)] <= 10)
            or np.any(ret5d[~np.isnan(ret5d)] <= -20)
            or np.any(intra[~np.isnan(intra)] >= 15)
            or np.any(vol_ratio[~np.isnan(vol_ratio)] >= 3)
            or np.any(dh52[~np.isnan(dh52)] >= -5))


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 2: PARAMETER GRID
# ═══════════════════════════════════════════════════════════════════════════════

def generate_param_grid():
    """6가지 전략 유형의 파라미터 조합 생성"""
    combos = []

    tp_small = [0.05, 0.07, 0.10]
    tp_mid = [0.05, 0.08, 0.10, 0.15, 0.20]
    tp_large = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40]

    # ── 1) RSI_OVERSOLD ──
    for rsi7_th in [15, 20, 25, 30]:
        for intra_th in [10, 15, 20, 25]:
            for ret_field, ret_vals in [('ret_1d', [-5, -8, -10]),
                                         ('ret_3d', [-10, -15, -20]),
                                         ('ret_5d', [-15, -20, -25, -30])]:
                for ret_th in ret_vals:
                    for down_th in [0, 3, 5]:
                        for tp in tp_mid:
                            for sl in [-0.15, -0.20]:
                                for mh in [5, 10, 15]:
                                    combos.append({
                                        'type': 'RSI_OVERSOLD',
                                        'rsi7_th': rsi7_th, 'intra_th': intra_th,
                                        'ret_field': ret_field, 'ret_th': ret_th,
                                        'down_th': down_th,
                                        'tp': tp, 'sl': sl, 'max_hold': mh,
                                    })

    # ── 2) PENNY_CRASH ──
    for price_max in [3, 5, 10]:
        for ret5d_th in [-25, -30, -40, -50]:
            for intra_th in [15, 20, 30]:
                for rsi14_th in [20, 25, 30]:
                    for tp in tp_large:
                        combos.append({
                            'type': 'PENNY_CRASH',
                            'price_max': price_max, 'ret5d_th': ret5d_th,
                            'intra_th': intra_th, 'rsi14_th': rsi14_th,
                            'tp': tp, 'sl': -0.20, 'max_hold': 30,
                        })

    # ── 3) VOLUME_SPIKE ──
    for rsi7_th in [20, 25, 30]:
        for vol_mult in [3, 5, 8]:
            for ret1d_th in [-5, -8, -10]:
                for intra_th in [10, 15, 20]:
                    for tp in tp_mid:
                        for sl in [-0.15, -0.20]:
                            combos.append({
                                'type': 'VOLUME_SPIKE',
                                'rsi7_th': rsi7_th, 'vol_mult': vol_mult,
                                'ret1d_th': ret1d_th, 'intra_th': intra_th,
                                'tp': tp, 'sl': sl, 'max_hold': 10,
                            })

    # ── 4) GAP_DOWN_RECOVERY ──
    for gap_th in [-5, -8, -10, -15]:
        for rsi7_th in [20, 25, 30, 35]:
            for intra_th in [8, 12, 15]:
                for tp in tp_small + [0.15]:
                    for sl in [-0.10, -0.15, -0.20]:
                        for mh in [3, 5, 7]:
                            combos.append({
                                'type': 'GAP_DOWN_RECOVERY',
                                'gap_th': gap_th, 'rsi7_th': rsi7_th,
                                'intra_th': intra_th,
                                'tp': tp, 'sl': sl, 'max_hold': mh,
                            })

    # ── 5) BB_SQUEEZE ──
    for bb_width_max in [5, 8, 10, 15]:
        for bb_pos_th in [-0.1, 0.0, 0.1]:
            for rsi14_th in [25, 30, 35]:
                for tp in tp_mid:
                    for sl in [-0.10, -0.15]:
                        for mh in [5, 10, 15]:
                            combos.append({
                                'type': 'BB_SQUEEZE',
                                'bb_width_max': bb_width_max, 'bb_pos_th': bb_pos_th,
                                'rsi14_th': rsi14_th,
                                'tp': tp, 'sl': sl, 'max_hold': mh,
                            })

    # ── 6) MOMENTUM_BREAKOUT ──
    for dist_high_th in [-3, -5, -8]:
        for vol_mult in [1.5, 2, 3]:
            for rsi14_min in [50, 55, 60]:
                for up_days_min in [0, 2, 3]:
                    for tp in tp_mid:
                        for sl in [-0.10, -0.15, -0.20]:
                            for mh in [5, 10, 15]:
                                combos.append({
                                    'type': 'MOMENTUM_BREAKOUT',
                                    'dist_high_th': dist_high_th,
                                    'vol_mult': vol_mult,
                                    'rsi14_min': rsi14_min,
                                    'up_days_min': up_days_min,
                                    'tp': tp, 'sl': sl, 'max_hold': mh,
                                })

    print(f"  총 파라미터 조합 수: {len(combos):,}개")
    return combos


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 3: SIGNAL SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

def scan_signals(ind, combo):
    """지표 데이터에서 전략 조건에 맞는 신호 인덱스 반환"""
    n = len(ind['close'])
    mask = np.ones(n, dtype=bool)
    mask[:60] = False  # 60일 워밍업
    ctype = combo['type']

    if ctype == 'RSI_OVERSOLD':
        rsi7 = ind['rsi7'].values
        intra = ind['intra'].values
        ret = ind[combo['ret_field']].values
        down = ind['down_days'].values
        mask &= ~np.isnan(rsi7) & ~np.isnan(ret) & ~np.isnan(intra)
        mask &= (rsi7 < combo['rsi7_th'])
        mask &= (intra > combo['intra_th'])
        mask &= (ret < combo['ret_th'])
        if combo['down_th'] > 0:
            mask &= (down >= combo['down_th'])

    elif ctype == 'PENNY_CRASH':
        price = ind['price'].values
        ret5d = ind['ret_5d'].values
        intra = ind['intra'].values
        rsi14 = ind['rsi14'].values
        mask &= ~np.isnan(ret5d) & ~np.isnan(rsi14) & ~np.isnan(intra)
        mask &= (price >= 1) & (price <= combo['price_max'])
        mask &= (ret5d <= combo['ret5d_th'])
        mask &= (intra >= combo['intra_th'])
        mask &= (rsi14 <= combo['rsi14_th'])

    elif ctype == 'VOLUME_SPIKE':
        rsi7 = ind['rsi7'].values
        vol = ind['vol'].values
        vol_ma5 = ind['vol_ma5'].values
        ret1d = ind['ret_1d'].values
        intra = ind['intra'].values
        mask &= ~np.isnan(rsi7) & ~np.isnan(ret1d) & ~np.isnan(vol_ma5)
        mask &= (vol_ma5 > 0)
        mask &= (rsi7 < combo['rsi7_th'])
        mask &= (vol >= vol_ma5 * combo['vol_mult'])
        mask &= (ret1d < combo['ret1d_th'])
        mask &= (intra > combo['intra_th'])

    elif ctype == 'GAP_DOWN_RECOVERY':
        gap = ind['gap_pct'].values
        rsi7 = ind['rsi7'].values
        intra = ind['intra'].values
        mask &= ~np.isnan(gap) & ~np.isnan(rsi7) & ~np.isnan(intra)
        mask &= (gap <= combo['gap_th'])
        mask &= (rsi7 < combo['rsi7_th'])
        mask &= (intra > combo['intra_th'])

    elif ctype == 'BB_SQUEEZE':
        bb_width = ind['bb_width'].values
        bb_pos = ind['bb_pos'].values
        rsi14 = ind['rsi14'].values
        mask &= ~np.isnan(bb_width) & ~np.isnan(bb_pos) & ~np.isnan(rsi14)
        mask &= (bb_width <= combo['bb_width_max'])
        mask &= (bb_pos <= combo['bb_pos_th'])
        mask &= (rsi14 <= combo['rsi14_th'])

    elif ctype == 'MOMENTUM_BREAKOUT':
        dist_h52 = ind['dist_high52w'].values
        vol_ratio = ind['vol_ratio'].values
        rsi14 = ind['rsi14'].values
        up_days = ind['up_days'].values
        mask &= ~np.isnan(dist_h52) & ~np.isnan(vol_ratio) & ~np.isnan(rsi14)
        mask &= (dist_h52 >= combo['dist_high_th'])
        mask &= (vol_ratio >= combo['vol_mult'])
        mask &= (rsi14 >= combo['rsi14_min'])
        if combo['up_days_min'] > 0:
            mask &= (up_days >= combo['up_days_min'])

    return np.where(mask)[0]


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 4: BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def backtest_signals(ind, signal_indices, tp, sl, max_hold):
    """
    백테스트 실행:
    - 진입: 신호일 다음날 시가 (D+1 Open)
    - TP/SL: 하루 내 둘 다 히트 → 보수적으로 SL 처리
    - 만기: 최종일 종가 청산 (양수면 WIN, 아니면 EXPIRED)
    """
    results = []
    close = ind['close'].values
    high = ind['high'].values
    low = ind['low'].values
    opn = ind['open'].values
    n = len(close)

    for idx in signal_indices:
        entry_idx = idx + 1
        if entry_idx >= n:
            continue

        ep = opn[entry_idx]
        if ep <= 0 or np.isnan(ep):
            ep = close[idx]
            if ep <= 0 or np.isnan(ep):
                continue

        tp_p = ep * (1 + tp)
        sl_p = ep * (1 + sl) if sl else 0

        result = None
        for d in range(0, max_hold):
            di = entry_idx + d
            if di >= n:
                break

            day_high = high[di]
            day_low = low[di]

            if np.isnan(day_high) or np.isnan(day_low):
                continue

            hit_tp = day_high >= tp_p
            hit_sl = sl_p > 0 and day_low <= sl_p

            if hit_tp and hit_sl:
                # 보수적 처리: SL 우선
                result = {'result': 'LOSS', 'pct': sl * 100, 'days': d}
                break
            elif hit_tp:
                result = {'result': 'WIN', 'pct': tp * 100, 'days': d}
                break
            elif hit_sl:
                result = {'result': 'LOSS', 'pct': sl * 100, 'days': d}
                break

        if result is None:
            last_di = min(entry_idx + max_hold - 1, n - 1)
            if last_di > entry_idx:
                exit_pct = (close[last_di] - ep) / ep * 100
                result = {
                    'result': 'WIN' if exit_pct > 0 else 'EXPIRED',
                    'pct': round(exit_pct, 2),
                    'days': last_di - entry_idx,
                }

        if result:
            results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 5: RESULT DISPLAY
# ═══════════════════════════════════════════════════════════════════════════════

def format_params(s):
    """전략 유형에 따라 파라미터를 읽기 좋게 포맷"""
    t = s['type']
    if t == 'RSI_OVERSOLD':
        return (f"RSI7<{s.get('rsi7_th')} | Intra>{s.get('intra_th')}% | "
                f"{s.get('ret_field')}<{s.get('ret_th')}% | Down>={s.get('down_th')}d")
    elif t == 'PENNY_CRASH':
        return (f"Price<=${s.get('price_max')} | Ret5d<={s.get('ret5d_th')}% | "
                f"Intra>={s.get('intra_th')}% | RSI14<={s.get('rsi14_th')}")
    elif t == 'VOLUME_SPIKE':
        return (f"RSI7<{s.get('rsi7_th')} | Vol>={s.get('vol_mult')}x | "
                f"Ret1d<{s.get('ret1d_th')}% | Intra>{s.get('intra_th')}%")
    elif t == 'GAP_DOWN_RECOVERY':
        return (f"Gap<={s.get('gap_th')}% | RSI7<{s.get('rsi7_th')} | "
                f"Intra>{s.get('intra_th')}%")
    elif t == 'BB_SQUEEZE':
        return (f"BB_Width<={s.get('bb_width_max')} | BB_Pos<={s.get('bb_pos_th')} | "
                f"RSI14<={s.get('rsi14_th')}")
    elif t == 'MOMENTUM_BREAKOUT':
        return (f"52wHigh>={s.get('dist_high_th')}% | Vol>={s.get('vol_mult')}x | "
                f"RSI14>={s.get('rsi14_min')} | Up>={s.get('up_days_min')}d")
    return str(s)


def print_detail(s):
    print(f"    WR: {s['win_rate']}% | EV: {s['ev']:+.2f}% | "
          f"Avg: {s['avg_pct']:+.1f}% | Med: {s['median_pct']:+.1f}% | "
          f"Days: {s['avg_days']:.1f}d")
    print(f"    Signals: {s['signals']}건 "
          f"({s['wins']}W/{s['losses']}L/{s['expired']}E) | "
          f"Tickers: {s['unique_tickers']}개")
    print(f"    TP: {s['tp']*100:.0f}% | SL: {s['sl']*100:.0f}% | Hold: {s['max_hold']}d")
    print(f"    {format_params(s)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    start_time = time.time()

    print("=" * 80)
    print("  Strategy Explorer v1 — Full-Spectrum Discovery")
    print(f"  기간: {DATA_PERIOD} | 최소 승률: {MIN_WIN_RATE}% | 최소 신호: {MIN_SIGNALS}건")
    print("  전략: RSI_OVERSOLD / PENNY_CRASH / VOLUME_SPIKE")
    print("        GAP_DOWN_RECOVERY / BB_SQUEEZE / MOMENTUM_BREAKOUT")
    print("=" * 80)
    sys.stdout.flush()

    # ── 1. 종목 수집 ──
    print("\n[1/4] 종목 수집...")
    try:
        tickers = get_all_tickers()
    except Exception as e:
        logger.warning(f"Ticker fetch failed: {e}")
        tickers = []
    sys.stdout.flush()

    if not tickers:
        print("ERROR: No tickers. Exiting.")
        return

    # ── 2. 5년 데이터 다운로드 ──
    print(f"\n[2/4] {DATA_PERIOD} 데이터 다운로드...")
    all_data = {}
    batch_size = 50
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for b_idx in range(0, len(tickers), batch_size):
        batch = tickers[b_idx:b_idx + batch_size]
        batch_num = b_idx // batch_size + 1

        if batch_num % 10 == 1 or batch_num == total_batches:
            elapsed = (time.time() - start_time) / 60
            print(f"  Batch {batch_num}/{total_batches} | "
                  f"{len(all_data)}종목 | {elapsed:.1f}분")
            sys.stdout.flush()

        try:
            data = download_batch(batch, period=DATA_PERIOD)
            if data is None or data.empty:
                time.sleep(1)
                continue
        except Exception as e:
            logger.warning(f"Batch {batch_num} failed: {e}")
            time.sleep(1)
            continue

        for tk in batch:
            df = extract_ticker_df(data, tk, len(batch))
            if df is None:
                continue
            try:
                df = df.dropna(how='all')
                if len(df) < 120:
                    continue
                ind = precompute_indicators(df)
                all_data[tk] = ind
            except Exception as e:
                logger.warning(f"Indicators failed for {tk}: {e}")
                continue
        time.sleep(0.2)

    elapsed = (time.time() - start_time) / 60
    print(f"\n  완료: {len(all_data)}개 종목 ({elapsed:.1f}분)")
    sys.stdout.flush()

    # ── 3. 필터링 ──
    print(f"\n[3/4] 필터링...")
    filtered = {tk: ind for tk, ind in all_data.items()
                if ticker_has_potential(ind)}
    print(f"  {len(all_data)}개 → {len(filtered)}개 통과")
    sys.stdout.flush()

    # ── 4. 그리드 서치 ──
    combos = generate_param_grid()
    print(f"\n[4/4] 그리드 서치")
    print(f"  {len(combos):,}개 조합 × {len(filtered)}개 종목")
    print(f"{'='*80}")
    sys.stdout.flush()

    best = []           # 90%+ WR
    decent = []         # 80-89% WR
    all_summary = []
    best_per_type = {}

    total = len(combos)
    log_interval = max(total // 50, 1)
    last_idx = 0

    for c_idx, combo in enumerate(combos):
        elapsed = (time.time() - start_time) / 60
        if elapsed > MAX_RUNTIME_MINUTES:
            print(f"\n  ⏰ 시간 초과 ({elapsed:.1f}분) — 중단")
            break

        if c_idx % log_interval == 0:
            pct = c_idx / total * 100
            print(f"  {pct:.0f}% ({c_idx:,}/{total:,}) | "
                  f"90%+: {len(best)} | 80%+: {len(decent)} | {elapsed:.1f}분")
            sys.stdout.flush()

        all_results = []
        tickers_hit = set()

        for tk, ind in filtered.items():
            sig_idx = scan_signals(ind, combo)
            if len(sig_idx) == 0:
                continue
            results = backtest_signals(ind, sig_idx, combo['tp'], combo['sl'], combo['max_hold'])
            for r in results:
                r['ticker'] = tk
                tickers_hit.add(tk)
            all_results.extend(results)

        n = len(all_results)
        if n < MIN_SIGNALS:
            last_idx = c_idx
            continue

        wins = sum(1 for r in all_results if r['result'] == 'WIN')
        losses = sum(1 for r in all_results if r['result'] == 'LOSS')
        expired = sum(1 for r in all_results if r['result'] == 'EXPIRED')
        decided = wins + losses + expired
        if decided == 0:
            last_idx = c_idx
            continue

        win_rate = wins / decided * 100
        pcts = [r['pct'] for r in all_results]
        avg_pct = np.mean(pcts)
        median_pct = np.median(pcts)
        avg_days = np.mean([r['days'] for r in all_results])

        win_pcts = [r['pct'] for r in all_results if r['result'] == 'WIN']
        loss_pcts = [r['pct'] for r in all_results if r['result'] != 'WIN']
        avg_win = np.mean(win_pcts) if win_pcts else 0
        avg_loss = np.mean(loss_pcts) if loss_pcts else 0
        ev = (win_rate / 100) * avg_win + (1 - win_rate / 100) * avg_loss

        summary = {
            'type': combo['type'],
            'signals': n, 'wins': wins, 'losses': losses, 'expired': expired,
            'unique_tickers': len(tickers_hit),
            'win_rate': round(win_rate, 1),
            'avg_pct': round(avg_pct, 2),
            'median_pct': round(median_pct, 2),
            'avg_days': round(avg_days, 1),
            'ev': round(ev, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'tp': combo['tp'], 'sl': combo['sl'], 'max_hold': combo['max_hold'],
        }

        for key in ['rsi7_th', 'rsi14_th', 'intra_th', 'ret_field', 'ret_th',
                     'down_th', 'price_max', 'ret5d_th', 'vol_mult', 'ret1d_th',
                     'gap_th', 'bb_width_max', 'bb_pos_th', 'dist_high_th',
                     'rsi14_min', 'up_days_min']:
            if key in combo:
                summary[key] = combo[key]

        if win_rate >= MIN_WIN_RATE:
            best.append(summary)
            ctype = combo['type']
            prev_ev = best_per_type.get(ctype, {}).get('ev', -999)
            if ev > prev_ev:
                best_per_type[ctype] = summary
                if ev > 2:
                    print(f"\n  ★ [{ctype}] WR={win_rate:.0f}% TP={combo['tp']*100:.0f}% "
                          f"EV={ev:+.2f}% Sig={n} Tk={len(tickers_hit)}")
                    sys.stdout.flush()
        elif win_rate >= 80:
            decent.append(summary)

        all_summary.append(summary)
        last_idx = c_idx

    # ── Results ──
    total_elapsed = (time.time() - start_time) / 60
    print(f"\n{'='*80}")
    print(f"  완료! ({total_elapsed:.1f}분)")
    print(f"{'='*80}")
    print(f"  종목: {len(all_data)} → {len(filtered)}개")
    print(f"  조합: {min(last_idx+1, total):,}/{total:,}개 실행")
    print(f"  ★ 90%+: {len(best)}개 | ◎ 80%+: {len(decent)}개")
    print(f"  유효 (신호 {MIN_SIGNALS}건+): {len(all_summary)}개")

    if best:
        # 유형별 Top 5
        type_groups = {}
        for s in best:
            type_groups.setdefault(s['type'], []).append(s)

        print(f"\n{'='*80}")
        print(f"  ★ 전략 유형별 TOP 5 (승률 90%+, EV 순)")
        print(f"{'='*80}")

        for t in sorted(type_groups.keys()):
            items = sorted(type_groups[t], key=lambda x: x['ev'], reverse=True)
            print(f"\n  ─── {t} ({len(items)}개) ───")
            for i, s in enumerate(items[:5]):
                print(f"\n  [{i+1}] EV={s['ev']:+.2f}% | TP={s['tp']*100:.0f}%")
                print_detail(s)

        # Overall Top 20
        print(f"\n{'='*80}")
        print(f"  ★ Overall TOP 20 (Score = EV × √Signals)")
        print(f"{'='*80}")
        scored = sorted(best,
                       key=lambda x: x['ev'] * (x['signals']**0.5) if x['ev'] > 0 else 0,
                       reverse=True)
        for i, s in enumerate(scored[:20]):
            score = s['ev'] * (s['signals']**0.5) if s['ev'] > 0 else 0
            print(f"\n  [{i+1}] {s['type']} | Score={score:.1f} | EV={s['ev']:+.2f}%")
            print_detail(s)

    elif decent:
        print(f"\n  90%+ 없음 — 80%+ TOP 10:")
        for i, s in enumerate(sorted(decent, key=lambda x: x['ev'], reverse=True)[:10]):
            print(f"\n  [{i+1}] {s['type']} | EV={s['ev']:+.2f}%")
            print_detail(s)

    # ── Save ──
    os.makedirs('data', exist_ok=True)

    if best:
        df = pd.DataFrame(best).sort_values('ev', ascending=False)
        df.to_csv('data/explorer_best.csv', index=False)
        print(f"\n★ data/explorer_best.csv ({len(best)}개)")

    if decent:
        df = pd.DataFrame(decent).sort_values('ev', ascending=False)
        df.to_csv('data/explorer_decent.csv', index=False)
        print(f"◎ data/explorer_decent.csv ({len(decent)}개)")

    if all_summary:
        df = pd.DataFrame(all_summary).sort_values('ev', ascending=False).head(500)
        df.to_csv('data/explorer_all_top500.csv', index=False)
        print(f"전체 상위 500: data/explorer_all_top500.csv")

    summary_json = {
        'run_date': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'data_period': DATA_PERIOD,
        'tickers_total': len(all_data),
        'tickers_filtered': len(filtered),
        'combos_total': total,
        'combos_tested': min(last_idx + 1, total),
        'best_90pct': len(best),
        'decent_80pct': len(decent),
        'runtime_min': round(total_elapsed, 1),
        'best_per_type': {
            t: {'ev': s['ev'], 'win_rate': s['win_rate'],
                'tp': s['tp'], 'signals': s['signals']}
            for t, s in best_per_type.items()
        },
    }
    with open('data/explorer_summary.json', 'w') as f:
        json.dump(summary_json, f, indent=2)
    print(f"\n요약: data/explorer_summary.json")
    print(f"총 소요: {total_elapsed:.1f}분")


if __name__ == '__main__':
    main()
