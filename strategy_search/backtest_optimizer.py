#!/usr/bin/env python3
"""
전략 자동 탐색기 v4 (Grid Search Optimizer)
- 유니버스: NASDAQ/NYSE/AMEX 전체 10,000개+
- 목표: 승률 90% 이상 유지하면서 TP가 가장 높은 조합 찾기
- TP 범위: 5% ~ 50% (세분화)
- 최소 신호 10건 이상만 유효
- GitHub Actions 6시간 타임아웃
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
import warnings
import sys
import logging
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─── Shared Imports ──────────────────────────────────────────────────────────
import os as _os; sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), '..', 'core'))
from shared_config import (
    LEVERAGED_ETF,
    calc_rsi_wilder,
    get_all_tickers,
    download_batch,
    extract_ticker_df,
)

MIN_SIGNALS = 100  # 최소 100건 (월 4건 × 2년 ≈ 96건)
MAX_RUNTIME_MINUTES = 330  # 5.5 hours (30min safety margin from 6-hour timeout)

# ─── 지표 ────────────────────────────────────────────────────────────────────

def consecutive_down_days(close):
    is_down = close < close.shift(1)
    groups = (~is_down).cumsum()
    return is_down.groupby(groups).cumsum().astype(int)

def precompute_indicators(df):
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    vol = df['Volume'].astype(float)

    ind = {
        'close': close, 'high': high, 'low': low, 'vol': vol,
        'rsi7': calc_rsi_wilder(close, 7),
        'rsi14': calc_rsi_wilder(close, 14),
        'ret_1d': close.pct_change(1) * 100,
        'ret_3d': close.pct_change(3) * 100,
        'ret_5d': close.pct_change(5) * 100,
        'intra': (high - low) / low * 100,
        'down_days': consecutive_down_days(close),
        'price': close,
    }

    low_5d = low.rolling(5).min()
    ind['dist_low5'] = (close - low_5d) / low_5d * 100
    ind['vol_ma5'] = vol.rolling(5).mean()
    ind['vol_ma20'] = vol.rolling(20).mean()

    return ind

def ticker_has_potential(ind):
    """이 종목이 과매도 반등 신호를 낼 가능성이 있는지 체크"""
    rsi7 = ind['rsi7'].values
    price = ind['price'].values
    ret5d = ind['ret_5d'].values
    intra = ind['intra'].values

    # RSI7이 한번이라도 35 미만이었거나
    has_low_rsi = np.any(rsi7[~np.isnan(rsi7)] < 35)
    # 가격이 $10 이하였거나
    has_low_price = np.any(price[~np.isnan(price)] <= 10)
    # 5일 수익률이 -20% 이하였거나
    has_big_drop = np.any(ret5d[~np.isnan(ret5d)] <= -20)
    # 일중 변동 15% 이상이 한번이라도 있었거나
    has_high_vol = np.any(intra[~np.isnan(intra)] >= 15)

    return has_low_rsi or has_low_price or has_big_drop or has_high_vol

# ─── 파라미터 그리드 ─────────────────────────────────────────────────────────

def generate_param_grid():
    """승률 90% 유지하면서 최대 TP 찾기 — TP를 5%~50% 세분화"""
    combos = []

    # TP 범위: 5%부터 50%까지 촘촘하게
    tp_range = [0.05, 0.07, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50]

    # ── 그룹 1: RSI7 과매도 반등 (기존 A/C 계열) ──
    for rsi7_th in [15, 20, 25, 30]:
        for intra_th in [10, 15, 20]:
            for ret_period, ret_vals in [('ret_1d', [-5, -8, -10]),
                                          ('ret_3d', [-10, -15, -20]),
                                          ('ret_5d', [-15, -20, -25])]:
                for ret_th in ret_vals:
                    for down_th in [0, 3, 5]:
                        for tp in tp_range:
                            combos.append({
                                'type': 'RSI7_OVERSOLD',
                                'rsi7_th': rsi7_th, 'intra_th': intra_th,
                                'ret_field': ret_period, 'ret_th': ret_th,
                                'down_th': down_th,
                                'tp': tp, 'sl': -0.20, 'max_hold': 30,
                            })

    # ── 그룹 2: 저가주 폭락 반등 (기존 D/E 계열) ──
    for price_max in [3, 5, 10]:
        for ret5d_th in [-25, -30, -35, -40, -50]:
            for intra_th in [15, 20, 25, 30]:
                for rsi14_th in [15, 20, 25, 30]:
                    for tp in tp_range:
                        combos.append({
                            'type': 'PENNY_CRASH',
                            'price_max': price_max, 'ret5d_th': ret5d_th,
                            'intra_th': intra_th, 'rsi14_th': rsi14_th,
                            'tp': tp, 'sl': -0.20, 'max_hold': 30,
                        })

    # ── 그룹 3: 거래량 폭증 + 과매도 ──
    for rsi7_th in [15, 20, 25, 30]:
        for vol_mult in [3, 5, 8]:
            for ret1d_th in [-5, -8, -10]:
                for intra_th in [10, 15, 20]:
                    for tp in tp_range:
                        combos.append({
                            'type': 'VOLUME_SPIKE',
                            'rsi7_th': rsi7_th, 'vol_mult': vol_mult,
                            'ret1d_th': ret1d_th, 'intra_th': intra_th,
                            'tp': tp, 'sl': -0.20, 'max_hold': 30,
                        })

    print(f"  총 파라미터 조합 수: {len(combos):,}개")
    return combos

# ─── 신호 스캔 ───────────────────────────────────────────────────────────────

def scan_signals(ind, combo):
    n = len(ind['close'])
    mask = np.ones(n, dtype=bool)
    mask[:30] = False
    ctype = combo['type']

    if ctype == 'RSI7_OVERSOLD':
        rsi7 = ind['rsi7'].values
        intra = ind['intra'].values
        ret = ind[combo['ret_field']].values
        down = ind['down_days'].values
        mask &= ~np.isnan(rsi7) & ~np.isnan(ret)
        mask &= (rsi7 < combo['rsi7_th']) & (intra > combo['intra_th']) & (ret < combo['ret_th'])
        if combo['down_th'] > 0:
            mask &= down >= combo['down_th']

    elif ctype == 'PENNY_CRASH':
        price = ind['price'].values
        ret5d = ind['ret_5d'].values
        intra = ind['intra'].values
        rsi14 = ind['rsi14'].values
        mask &= ~np.isnan(ret5d) & ~np.isnan(rsi14)
        mask &= (price >= 1) & (price <= combo['price_max'])
        mask &= (ret5d <= combo['ret5d_th']) & (intra >= combo['intra_th']) & (rsi14 <= combo['rsi14_th'])

    elif ctype == 'VOLUME_SPIKE':
        rsi7 = ind['rsi7'].values
        vol = ind['vol'].values
        vol_ma5 = ind['vol_ma5'].values
        ret1d = ind['ret_1d'].values
        intra = ind['intra'].values
        mask &= ~np.isnan(rsi7) & ~np.isnan(ret1d) & ~np.isnan(vol_ma5) & (vol_ma5 > 0)
        mask &= (rsi7 < combo['rsi7_th']) & (vol >= vol_ma5 * combo['vol_mult'])
        mask &= (ret1d < combo['ret1d_th']) & (intra > combo['intra_th'])

    return np.where(mask)[0]

# ─── 백테스트 ────────────────────────────────────────────────────────────────

def backtest_signals_fast(ind, signal_indices, tp, sl, max_hold):
    results = []
    close = ind['close'].values
    high = ind['high'].values
    low = ind['low'].values
    n = len(close)

    for idx in signal_indices:
        ep = close[idx]
        if ep <= 0 or np.isnan(ep):
            continue
        tp_p = ep * (1 + tp)
        sl_p = ep * (1 + sl)

        result = None
        for d in range(1, max_hold + 1):
            di = idx + d
            if di >= n:
                break
            if high[di] >= tp_p:
                result = {'result': 'WIN', 'pct': tp * 100, 'days': d}
                break
            if low[di] <= sl_p:
                result = {'result': 'LOSS', 'pct': sl * 100, 'days': d}
                break

        if result is None:
            last_di = min(idx + max_hold, n - 1)
            if last_di > idx:
                exit_pct = (close[last_di] - ep) / ep * 100
                result = {'result': 'WIN' if exit_pct > 0 else 'EXPIRED',
                         'pct': exit_pct, 'days': last_di - idx}

        if result:
            results.append(result)
    return results

# ─── 메인 ────────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()

    print("=" * 80)
    print("  전략 자동 탐색기 v4 (전종목 3000+ Grid Search)")
    print("  목표: 승률 90%+ & 평균수익률 50%+")
    print("  기간: 최근 2년 | 최소 신호: 100건")
    print("=" * 80)
    sys.stdout.flush()

    # ── Step 1: 종목 수집 ──
    print("\n[Step 1/4] 종목 수집 중...")
    try:
        tickers = get_all_tickers()
        print(f"  → {len(tickers)}개 종목")
    except Exception as e:
        logger.warning(f"Failed to get tickers: {e}")
        print("  → 종목 수집 실패, 기본 목록 사용")
        tickers = []
    sys.stdout.flush()

    if not tickers:
        print("Error: No tickers available")
        return

    # ── Step 2: 2년치 데이터 다운로드 + 지표 계산 ──
    print(f"\n[Step 2/4] 2년치 데이터 다운로드 중...")
    all_data = {}
    batch_size = 50  # 큰 배치로 속도 향상
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    failed = 0

    for b_idx in range(0, len(tickers), batch_size):
        batch = tickers[b_idx:b_idx + batch_size]
        batch_num = b_idx // batch_size + 1

        if batch_num % 10 == 1 or batch_num == total_batches:
            elapsed = (time.time() - start_time) / 60
            print(f"  Batch {batch_num}/{total_batches} | "
                  f"{len(all_data)}종목 완료 | {elapsed:.1f}분 경과")
            sys.stdout.flush()

        try:
            data = download_batch(batch, period='2y')
            if data is None or data.empty:
                failed += len(batch)
                time.sleep(1)
                continue
        except Exception as e:
            logger.warning(f"Batch download failed for batch {batch_num}: {e}")
            failed += len(batch)
            time.sleep(1)
            continue

        for tk in batch:
            df = extract_ticker_df(data, tk, len(batch))
            if df is None:
                continue
            try:
                df = df.dropna(how='all')
                if len(df) < 50:
                    continue
                ind = precompute_indicators(df)
                all_data[tk] = ind
            except Exception as e:
                logger.warning(f"Failed to precompute indicators for {tk}: {e}")
                continue

        time.sleep(0.2)

    elapsed = (time.time() - start_time) / 60
    print(f"\n  다운로드 완료: {len(all_data)}개 종목 ({elapsed:.1f}분)")
    sys.stdout.flush()

    # ── Step 3: 과매도 경험 종목만 필터링 ──
    print(f"\n[Step 3/4] 신호 가능 종목 필터링...")
    filtered_data = {}
    for tk, ind in all_data.items():
        if ticker_has_potential(ind):
            filtered_data[tk] = ind

    print(f"  → {len(all_data)}개 중 {len(filtered_data)}개 종목이 신호 가능")
    print(f"  → {len(all_data) - len(filtered_data)}개 종목 제외 (과매도 경험 없음)")
    sys.stdout.flush()

    # ── Step 4: 그리드 서치 ──
    combos = generate_param_grid()

    print(f"\n[Step 4/4] 그리드 서치 시작")
    print(f"  {len(combos):,}개 조합 × {len(filtered_data)}개 종목")
    print(f"{'='*80}")
    sys.stdout.flush()

    good_results = []
    decent_results = []
    all_summary = []

    total_combos = len(combos)
    check_interval = max(total_combos // 20, 1)

    for c_idx, combo in enumerate(combos):
        # Check time budget at start of each combo iteration
        elapsed = (time.time() - start_time) / 60
        if elapsed > MAX_RUNTIME_MINUTES:
            print(f"\n  WARNING: Time budget exceeded ({elapsed:.1f}min > {MAX_RUNTIME_MINUTES}min)")
            print(f"  Stopping grid search early, saving results...")
            break

        if c_idx % check_interval == 0:
            pct = c_idx / total_combos * 100
            print(f"  진행: {pct:.0f}% ({c_idx:,}/{total_combos:,}) | "
                  f"최고: {len(good_results)}개 | 괜찮은: {len(decent_results)}개 | "
                  f"{elapsed:.1f}분 경과")
            sys.stdout.flush()

        all_results = []
        tickers_hit = set()

        for tk, ind in filtered_data.items():
            sig_idx = scan_signals(ind, combo)
            if len(sig_idx) == 0:
                continue
            results = backtest_signals_fast(ind, sig_idx, combo['tp'], combo['sl'], combo['max_hold'])
            for r in results:
                r['ticker'] = tk
                tickers_hit.add(tk)
            all_results.extend(results)

        n = len(all_results)
        if n < MIN_SIGNALS:
            continue

        wins = sum(1 for r in all_results if r['result'] == 'WIN')
        pcts = [r['pct'] for r in all_results]
        win_rate = wins / n * 100
        avg_pct = np.mean(pcts)
        median_pct = np.median(pcts)
        unique_tickers = len(tickers_hit)

        summary = {
            'combo_idx': c_idx, 'type': combo['type'],
            'signals': n, 'unique_tickers': unique_tickers,
            'win_rate': round(win_rate, 1),
            'avg_pct': round(avg_pct, 2), 'median_pct': round(median_pct, 2),
            'tp': combo['tp'], 'sl': combo['sl'], 'max_hold': combo['max_hold'],
        }

        # 파라미터 저장
        if combo['type'] == 'RSI7_OVERSOLD':
            summary.update({
                'rsi7_th': combo['rsi7_th'], 'intra_th': combo['intra_th'],
                'ret_field': combo['ret_field'], 'ret_th': combo['ret_th'],
                'down_th': combo['down_th'],
            })
        elif combo['type'] == 'PENNY_CRASH':
            summary.update({
                'price_max': combo['price_max'], 'ret5d_th': combo['ret5d_th'],
                'intra_th': combo['intra_th'], 'rsi14_th': combo['rsi14_th'],
            })
        elif combo['type'] == 'VOLUME_SPIKE':
            summary.update({
                'rsi7_th': combo['rsi7_th'], 'vol_mult': combo['vol_mult'],
                'ret1d_th': combo['ret1d_th'], 'intra_th': combo['intra_th'],
            })

        if win_rate >= 90:
            good_results.append(summary)
            if combo['tp'] >= 0.20:  # TP 20% 이상이면 하이라이트
                print(f"\n  ★★★ 승률90%+고TP! [{combo['type']}] "
                      f"승률={win_rate:.0f}% TP={combo['tp']*100:.0f}% "
                      f"평균={avg_pct:+.1f}% 신호={n}건 종목={unique_tickers}개")
                sys.stdout.flush()
        elif win_rate >= 80:
            decent_results.append(summary)

        all_summary.append(summary)

    # ── 최종 결과 ──
    total_elapsed = (time.time() - start_time) / 60
    print(f"\n{'='*80}")
    print(f"  그리드 서치 완료! (총 {total_elapsed:.1f}분)")
    print(f"{'='*80}")
    print(f"  전체 종목: {len(all_data)}개 → 필터 후: {len(filtered_data)}개")
    print(f"  총 조합 테스트: {len(combos):,}개 중 {c_idx+1:,}개 실행")
    print(f"  ★ 승률 90%+ 조합: {len(good_results)}개")
    print(f"  ◎ 승률 80%+ 조합: {len(decent_results)}개")
    print(f"  전체 유효 조합 (신호{MIN_SIGNALS}건+): {len(all_summary)}개")

    def print_detail(s):
        print(f"    승률: {s['win_rate']}% | 평균: {s['avg_pct']:+.1f}% | "
              f"중간: {s['median_pct']:+.1f}% | 신호: {s['signals']}건 | 종목: {s['unique_tickers']}개")
        print(f"    TP: {s['tp']*100:.0f}% | SL: {s['sl']*100:.0f}% | 보유: {s['max_hold']}일")
        if s['type'] == 'RSI7_OVERSOLD':
            print(f"    RSI7<{s.get('rsi7_th')} | 일중>{s.get('intra_th')}% | "
                  f"{s.get('ret_field')}<{s.get('ret_th')}% | 연속하락>={s.get('down_th')}일")
        elif s['type'] == 'PENNY_CRASH':
            print(f"    가격<=${s.get('price_max')} | 5일수익<={s.get('ret5d_th')}% | "
                  f"일중>={s.get('intra_th')}% | RSI14<={s.get('rsi14_th')}")
        elif s['type'] == 'VOLUME_SPIKE':
            print(f"    RSI7<{s.get('rsi7_th')} | 거래량>={s.get('vol_mult')}배 | "
                  f"1일수익<{s.get('ret1d_th')}% | 일중>{s.get('intra_th')}%")

    if good_results:
        # TP 기준 내림차순 정렬 (승률 90%+ 중 TP 가장 높은 순)
        print(f"\n{'='*80}")
        print(f"  ★★★ 승률 90%+ 조합 — TP 높은 순 TOP 30")
        print(f"{'='*80}")
        # 점수 = TP × 신호수 × 승률 (TP 높고, 신호 많고, 승률 높은 순)
        good_sorted = sorted(good_results, key=lambda x: x['tp'] * x['signals'] * x['win_rate'], reverse=True)
        for i, s in enumerate(good_sorted[:30]):
            score = s['tp'] * s['signals'] * s['win_rate']
            print(f"\n  [{i+1}위] {s['type']} | TP={s['tp']*100:.0f}% | 신호={s['signals']}건 | 점수={score:.0f}")
            print_detail(s)

        # TP별 최고 승률 요약
        print(f"\n{'='*80}")
        print(f"  TP별 최고 승률 요약 (승률 90%+ 조합만)")
        print(f"{'='*80}")
        tp_groups = {}
        for s in good_results:
            tp_key = f"{s['tp']*100:.0f}%"
            if tp_key not in tp_groups:
                tp_groups[tp_key] = []
            tp_groups[tp_key].append(s)
        for tp_key in sorted(tp_groups.keys(), key=lambda x: float(x.replace('%','')), reverse=True):
            items = tp_groups[tp_key]
            best = max(items, key=lambda x: x['win_rate'])
            print(f"  TP {tp_key}: {len(items)}개 조합 | 최고 승률 {best['win_rate']}% | "
                  f"최다 신호 {max(s['signals'] for s in items)}건")

    if decent_results:
        print(f"\n{'='*80}")
        print(f"  ◎ 승률 80%+ 조합 — TP 높은 순 TOP 20")
        print(f"{'='*80}")
        for i, s in enumerate(sorted(decent_results, key=lambda x: (x['tp'], x['win_rate']), reverse=True)[:20]):
            print(f"\n  [{i+1}위] {s['type']} | TP={s['tp']*100:.0f}%")
            print_detail(s)

    if not good_results and not decent_results and all_summary:
        print(f"\n{'='*80}")
        print(f"  목표 미달 — 상위 20개 조합:")
        print(f"{'='*80}")
        for i, s in enumerate(sorted(all_summary, key=lambda x: x['win_rate']*max(x['avg_pct'],0.01), reverse=True)[:20]):
            print(f"\n  [{i+1}위] {s['type']}")
            print_detail(s)

    # ── CSV 저장 ──
    if good_results:
        try:
            pd.DataFrame(good_results).to_csv('optimizer_best.csv', index=False)
            print(f"\n★ 최고조건 저장: optimizer_best.csv")
        except Exception as e:
            logger.warning(f"Failed to save optimizer_best.csv: {e}")
    if decent_results:
        try:
            pd.DataFrame(decent_results).to_csv('optimizer_decent.csv', index=False)
            print(f"◎ 괜찮은조건 저장: optimizer_decent.csv")
        except Exception as e:
            logger.warning(f"Failed to save optimizer_decent.csv: {e}")
    if all_summary:
        try:
            df_all = pd.DataFrame(all_summary)
            df_all = df_all.sort_values('win_rate', ascending=False).head(200)
            df_all.to_csv('optimizer_all_top100.csv', index=False)
            print(f"전체 상위200 저장: optimizer_all_top100.csv")
        except Exception as e:
            logger.warning(f"Failed to save optimizer_all_top100.csv: {e}")

    print(f"\n총 소요시간: {total_elapsed:.1f}분")

if __name__ == '__main__':
    main()
