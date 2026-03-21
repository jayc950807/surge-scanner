#!/usr/bin/env python3
"""
전략 자동 탐색기 (Grid Search Optimizer)
- RSI, 수익률, 일중변동, 연속하락, 거래량 등 파라미터 조합을 자동 생성
- 각 조합별 백테스트 실행
- 승률 90%+ & 평균수익률 50%+ 조합만 필터링
- GitHub Actions에서 실행 (약 10~20분 소요)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
import warnings
import itertools
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ─── 유니버스 ─────────────────────────────────────────────────────────────────

def get_sp500_tickers():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        df = tables[0]
        return df['Symbol'].str.replace('.', '-', regex=False).tolist()
    except:
        return ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','JPM','V','JNJ',
                'WMT','PG','MA','UNH','HD','DIS','BAC','NFLX','ADBE','CRM',
                'PYPL','INTC','AMD','CSCO','PEP','KO','MRK','ABT','TMO','AVGO',
                'COST','NKE','ACN','LLY','ORCL','TXN','QCOM','LOW','UPS','BA']

def get_extra_volatile():
    return ['SOFI','PLTR','RIVN','LCID','NIO','SNAP','PINS','ROKU','CRWD','DDOG',
            'NET','RBLX','HOOD','COIN','MARA','RIOT','UPST','AFRM','GSAT','BB',
            'NOK','SNDL','TLRY','CGC','ACB','PLUG','FCEL','BLNK','CHPT','QS',
            'NKLA','WKHS','DKNG','PENN','MGNI','FUBO','OPEN','LMND','ROOT',
            'SMCI','IONQ','RGTI','QUBT','KULR','OKLO','LUNR','RKLB','ASTS','APLD',
            'MSTR','AMC','GME','EXPR','CLOV','SKLZ','GOEV','MULN',
            'PHUN','BKKT','IRNT','ATER','PROG','XELA']

# ─── 지표 계산 (벡터화 — 전 종목 한번에) ─────────────────────────────────────

def calc_rsi(series, period):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def consecutive_down_days(close):
    is_down = close < close.shift(1)
    groups = (~is_down).cumsum()
    return is_down.groupby(groups).cumsum().astype(int)

def precompute_indicators(df):
    """모든 지표를 한번에 계산 (종목별 1회)"""
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    vol = df['Volume'].astype(float)

    ind = {}
    ind['close'] = close
    ind['high'] = high
    ind['low'] = low
    ind['vol'] = vol

    # RSI
    ind['rsi7'] = calc_rsi(close, 7)
    ind['rsi14'] = calc_rsi(close, 14)

    # 수익률
    ind['ret_1d'] = close.pct_change(1) * 100
    ind['ret_3d'] = close.pct_change(3) * 100
    ind['ret_5d'] = close.pct_change(5) * 100

    # 일중 변동폭
    ind['intra'] = (high - low) / low * 100

    # 연속 하락일
    ind['down_days'] = consecutive_down_days(close)

    # 5일 저점 거리
    low_5d = low.rolling(5).min()
    ind['dist_low5'] = (close - low_5d) / low_5d * 100

    # 거래량 이동평균
    ind['vol_ma5'] = vol.rolling(5).mean()
    ind['vol_ma20'] = vol.rolling(20).mean()

    # 가격
    ind['price'] = close

    return ind

# ─── 파라미터 그리드 정의 ─────────────────────────────────────────────────────

def generate_param_grid():
    """
    과매도 반등 전략의 파라미터 조합 생성
    전략 구조: RSI 과매도 + 변동성 확인 + 추가 필터 → TP/SL/보유일 조합
    """
    combos = []

    # ── 그룹 1: RSI7 기반 과매도 반등 ──
    for rsi7_th in [10, 15, 20, 25, 30]:
        for intra_th in [10, 15, 20, 25]:
            for ret_period, ret_vals in [('ret_1d', [-5, -8, -10, -15]),
                                          ('ret_3d', [-10, -15, -20, -25]),
                                          ('ret_5d', [-15, -20, -25, -30])]:
                for ret_th in ret_vals:
                    for down_th in [0, 3, 5]:  # 0 = 조건 없음
                        for tp in [0.30, 0.40, 0.50]:
                            for sl in [-0.08, -0.10, -0.15]:
                                for max_hold in [10, 15, 20, 30]:
                                    combos.append({
                                        'type': 'RSI7_OVERSOLD',
                                        'rsi7_th': rsi7_th,
                                        'intra_th': intra_th,
                                        'ret_field': ret_period,
                                        'ret_th': ret_th,
                                        'down_th': down_th,
                                        'tp': tp, 'sl': sl,
                                        'max_hold': max_hold,
                                    })

    # ── 그룹 2: 저가주 폭락 반등 ($1~$10) ──
    for price_max in [5, 10]:
        for ret5d_th in [-25, -30, -35, -40, -50]:
            for intra_th in [15, 20, 25, 30]:
                for rsi14_th in [15, 20, 25, 30]:
                    for tp in [0.30, 0.40, 0.50, 0.60]:
                        for sl in [-0.10, -0.15, -0.20]:
                            for max_hold in [10, 15, 20, 30]:
                                combos.append({
                                    'type': 'PENNY_CRASH',
                                    'price_max': price_max,
                                    'ret5d_th': ret5d_th,
                                    'intra_th': intra_th,
                                    'rsi14_th': rsi14_th,
                                    'tp': tp, 'sl': sl,
                                    'max_hold': max_hold,
                                })

    # ── 그룹 3: 거래량 폭증 + 과매도 ──
    for rsi7_th in [15, 20, 25, 30]:
        for vol_mult in [3, 5, 8]:
            for ret1d_th in [-5, -8, -10]:
                for intra_th in [10, 15, 20]:
                    for tp in [0.30, 0.40, 0.50]:
                        for sl in [-0.08, -0.10, -0.15]:
                            for max_hold in [10, 15, 20]:
                                combos.append({
                                    'type': 'VOLUME_SPIKE',
                                    'rsi7_th': rsi7_th,
                                    'vol_mult': vol_mult,
                                    'ret1d_th': ret1d_th,
                                    'intra_th': intra_th,
                                    'tp': tp, 'sl': sl,
                                    'max_hold': max_hold,
                                })

    print(f"  총 파라미터 조합 수: {len(combos):,}개")
    return combos

# ─── 신호 스캔 (파라미터 기반) ────────────────────────────────────────────────

def scan_signals(ind, combo, df_index):
    """사전 계산된 지표(ind)와 파라미터 조합(combo)으로 신호 인덱스 반환"""
    n = len(ind['close'])
    mask = np.ones(n, dtype=bool)

    # 처음 30일은 지표 안정화 기간
    mask[:30] = False

    ctype = combo['type']

    if ctype == 'RSI7_OVERSOLD':
        rsi7 = ind['rsi7'].values
        intra = ind['intra'].values
        ret = ind[combo['ret_field']].values
        down = ind['down_days'].values

        mask &= ~np.isnan(rsi7)
        mask &= ~np.isnan(ret)
        mask &= rsi7 < combo['rsi7_th']
        mask &= intra > combo['intra_th']
        mask &= ret < combo['ret_th']
        if combo['down_th'] > 0:
            mask &= down >= combo['down_th']

    elif ctype == 'PENNY_CRASH':
        price = ind['price'].values
        ret5d = ind['ret_5d'].values
        intra = ind['intra'].values
        rsi14 = ind['rsi14'].values

        mask &= ~np.isnan(ret5d)
        mask &= ~np.isnan(rsi14)
        mask &= (price >= 1) & (price <= combo['price_max'])
        mask &= ret5d <= combo['ret5d_th']
        mask &= intra >= combo['intra_th']
        mask &= rsi14 <= combo['rsi14_th']

    elif ctype == 'VOLUME_SPIKE':
        rsi7 = ind['rsi7'].values
        vol = ind['vol'].values
        vol_ma5 = ind['vol_ma5'].values
        ret1d = ind['ret_1d'].values
        intra = ind['intra'].values

        mask &= ~np.isnan(rsi7)
        mask &= ~np.isnan(ret1d)
        mask &= ~np.isnan(vol_ma5)
        mask &= rsi7 < combo['rsi7_th']
        mask &= vol >= vol_ma5 * combo['vol_mult']
        mask &= ret1d < combo['ret1d_th']
        mask &= intra > combo['intra_th']

    signal_indices = np.where(mask)[0]
    return signal_indices

# ─── 백테스트 (벡터화) ───────────────────────────────────────────────────────

def backtest_signals_fast(ind, signal_indices, tp, sl, max_hold):
    """벡터화된 빠른 백테스트"""
    results = []
    close = ind['close'].values
    high = ind['high'].values
    low = ind['low'].values
    n = len(close)

    for idx in signal_indices:
        entry_price = close[idx]
        if entry_price <= 0 or np.isnan(entry_price):
            continue

        tp_price = entry_price * (1 + tp)
        sl_price = entry_price * (1 + sl)

        result = None
        # D+1부터 체크
        for d in range(1, max_hold + 1):
            di = idx + d
            if di >= n:
                break

            if high[di] >= tp_price:
                result = {'result': 'WIN', 'pct': tp * 100, 'days': d}
                break
            if low[di] <= sl_price:
                result = {'result': 'LOSS', 'pct': sl * 100, 'days': d}
                break

        if result is None:
            # 타임아웃
            last_di = min(idx + max_hold, n - 1)
            if last_di > idx:
                exit_pct = (close[last_di] - entry_price) / entry_price * 100
                result = {
                    'result': 'WIN' if exit_pct > 0 else 'EXPIRED',
                    'pct': exit_pct,
                    'days': last_di - idx,
                }

        if result:
            results.append(result)

    return results

# ─── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("  전략 자동 탐색기 (Grid Search Optimizer)")
    print("  목표: 승률 90%+ & 평균수익률 50%+")
    print("  기간: 최근 2년 | 유니버스: S&P500 + 변동주")
    print("=" * 80)

    # ── 종목 수집 ──
    tickers = get_sp500_tickers() + get_extra_volatile()
    tickers = sorted(set(tickers))
    print(f"\n총 {len(tickers)}개 종목 대상")

    # ── 파라미터 그리드 ──
    combos = generate_param_grid()

    # ── 데이터 다운로드 + 지표 사전 계산 ──
    print(f"\n데이터 다운로드 중...")
    all_data = {}  # ticker -> (df, indicators)
    batch_size = 20
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for b_idx in range(0, len(tickers), batch_size):
        batch = tickers[b_idx:b_idx + batch_size]
        batch_num = b_idx // batch_size + 1

        if batch_num % 10 == 1 or batch_num == total_batches:
            print(f"  다운로드 Batch {batch_num}/{total_batches} ({len(all_data)}종목 완료)")

        try:
            data = yf.download(' '.join(batch), period='2y',
                             group_by='ticker', progress=False, threads=True, timeout=30)
        except:
            time.sleep(2)
            continue

        if data is None or data.empty:
            time.sleep(1)
            continue

        for tk in batch:
            try:
                if len(batch) == 1:
                    df = data
                else:
                    if isinstance(data.columns, pd.MultiIndex):
                        level_values = [set(data.columns.get_level_values(i))
                                        for i in range(data.columns.nlevels)]
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
                if len(df) < 50:
                    continue

                ind = precompute_indicators(df)
                all_data[tk] = (df, ind)
            except:
                continue

        time.sleep(0.3)

    print(f"\n총 {len(all_data)}개 종목 데이터 준비 완료")

    # ── 그리드 서치 실행 ──
    print(f"\n{'='*80}")
    print(f"  그리드 서치 시작: {len(combos):,}개 조합 × {len(all_data)}개 종목")
    print(f"{'='*80}")

    # 결과 저장용
    good_results = []     # 승률 90%+ & 수익률 50%+
    decent_results = []   # 승률 70%+ & 수익률 30%+
    all_summary = []

    total_combos = len(combos)
    check_interval = max(total_combos // 20, 1)

    for c_idx, combo in enumerate(combos):
        if c_idx % check_interval == 0:
            pct = c_idx / total_combos * 100
            print(f"  진행: {pct:.0f}% ({c_idx:,}/{total_combos:,}) | "
                  f"최고조건: {len(good_results)}개 | 괜찮은조건: {len(decent_results)}개")

        all_results = []

        for tk, (df, ind) in all_data.items():
            signal_indices = scan_signals(ind, combo, df.index)
            if len(signal_indices) == 0:
                continue

            results = backtest_signals_fast(
                ind, signal_indices,
                combo['tp'], combo['sl'], combo['max_hold']
            )
            for r in results:
                r['ticker'] = tk
            all_results.extend(results)

        # ── 결과 평가 ──
        n = len(all_results)
        if n < 3:  # 최소 3건 이상
            continue

        wins = sum(1 for r in all_results if r['result'] == 'WIN')
        pcts = [r['pct'] for r in all_results]
        win_rate = wins / n * 100
        avg_pct = np.mean(pcts)
        median_pct = np.median(pcts)

        summary = {
            'combo_idx': c_idx,
            'type': combo['type'],
            'params': str(combo),
            'signals': n,
            'win_rate': round(win_rate, 1),
            'avg_pct': round(avg_pct, 2),
            'median_pct': round(median_pct, 2),
            'tp': combo['tp'],
            'sl': combo['sl'],
            'max_hold': combo['max_hold'],
        }

        # 핵심 파라미터 추출
        if combo['type'] == 'RSI7_OVERSOLD':
            summary['rsi7_th'] = combo['rsi7_th']
            summary['intra_th'] = combo['intra_th']
            summary['ret_field'] = combo['ret_field']
            summary['ret_th'] = combo['ret_th']
            summary['down_th'] = combo['down_th']
        elif combo['type'] == 'PENNY_CRASH':
            summary['price_max'] = combo['price_max']
            summary['ret5d_th'] = combo['ret5d_th']
            summary['intra_th'] = combo['intra_th']
            summary['rsi14_th'] = combo['rsi14_th']
        elif combo['type'] == 'VOLUME_SPIKE':
            summary['rsi7_th'] = combo['rsi7_th']
            summary['vol_mult'] = combo['vol_mult']
            summary['ret1d_th'] = combo['ret1d_th']
            summary['intra_th'] = combo['intra_th']

        # 최고 조건: 승률 90%+ & 평균수익 50%+
        if win_rate >= 90 and avg_pct >= 50:
            good_results.append(summary)
            print(f"\n  ★★★ 최고조건 발견! [{combo['type']}] "
                  f"승률={win_rate:.0f}% 평균={avg_pct:+.1f}% 신호={n}건")

        # 괜찮은 조건: 승률 70%+ & 평균수익 30%+
        elif win_rate >= 70 and avg_pct >= 30:
            decent_results.append(summary)

        # 전체 기록 (신호 5건 이상만)
        if n >= 5:
            all_summary.append(summary)

    # ── 최종 결과 출력 ──
    print(f"\n{'='*80}")
    print(f"  그리드 서치 완료!")
    print(f"{'='*80}")
    print(f"  총 조합 테스트: {total_combos:,}개")
    print(f"  ★ 최고조건 (승률90%+ & 수익50%+): {len(good_results)}개")
    print(f"  ◎ 괜찮은조건 (승률70%+ & 수익30%+): {len(decent_results)}개")
    print(f"  전체 유효 조합 (신호5건+): {len(all_summary)}개")

    # ── 최고 조건 상세 출력 ──
    if good_results:
        print(f"\n{'='*80}")
        print(f"  ★★★ 최고 조건 TOP 20 (승률×수익 기준 정렬)")
        print(f"{'='*80}")
        good_sorted = sorted(good_results,
                            key=lambda x: x['win_rate'] * x['avg_pct'],
                            reverse=True)[:20]
        for i, s in enumerate(good_sorted):
            print(f"\n  [{i+1}위] {s['type']}")
            print(f"    승률: {s['win_rate']}% | 평균수익: {s['avg_pct']:+.1f}% | "
                  f"중간수익: {s['median_pct']:+.1f}% | 신호수: {s['signals']}건")
            print(f"    TP: {s['tp']*100:.0f}% | SL: {s['sl']*100:.0f}% | 보유: {s['max_hold']}일")
            # 타입별 파라미터
            if s['type'] == 'RSI7_OVERSOLD':
                print(f"    RSI7<{s.get('rsi7_th','-')} | 일중>{s.get('intra_th','-')}% | "
                      f"{s.get('ret_field','-')}<{s.get('ret_th','-')}% | 연속하락>={s.get('down_th','-')}일")
            elif s['type'] == 'PENNY_CRASH':
                print(f"    가격<=${s.get('price_max','-')} | 5일수익<={s.get('ret5d_th','-')}% | "
                      f"일중>={s.get('intra_th','-')}% | RSI14<={s.get('rsi14_th','-')}")
            elif s['type'] == 'VOLUME_SPIKE':
                print(f"    RSI7<{s.get('rsi7_th','-')} | 거래량>={s.get('vol_mult','-')}배 | "
                      f"1일수익<{s.get('ret1d_th','-')}% | 일중>{s.get('intra_th','-')}%")

    # ── 괜찮은 조건도 출력 ──
    if decent_results and not good_results:
        print(f"\n{'='*80}")
        print(f"  ◎ 괜찮은 조건 TOP 20")
        print(f"{'='*80}")
        decent_sorted = sorted(decent_results,
                              key=lambda x: x['win_rate'] * x['avg_pct'],
                              reverse=True)[:20]
        for i, s in enumerate(decent_sorted):
            print(f"\n  [{i+1}위] {s['type']}")
            print(f"    승률: {s['win_rate']}% | 평균수익: {s['avg_pct']:+.1f}% | 신호수: {s['signals']}건")
            print(f"    TP: {s['tp']*100:.0f}% | SL: {s['sl']*100:.0f}% | 보유: {s['max_hold']}일")

    # 최고조건 없으면 괜찮은 조건이라도 출력
    if not good_results and decent_results:
        print("\n  ※ 승률90%+수익50% 조건은 없었지만, 위 조건들이 차선입니다.")

    if not good_results and not decent_results:
        # 전체 중 최상위라도 보여줌
        if all_summary:
            print(f"\n{'='*80}")
            print(f"  목표 미달이지만 상위 20개 조합:")
            print(f"{'='*80}")
            top_all = sorted(all_summary,
                            key=lambda x: x['win_rate'] * max(x['avg_pct'], 0.01),
                            reverse=True)[:20]
            for i, s in enumerate(top_all):
                print(f"  {i+1}. [{s['type']}] 승률={s['win_rate']}% 수익={s['avg_pct']:+.1f}% 신호={s['signals']}건")

    # ── CSV 저장 ──
    if good_results:
        pd.DataFrame(good_results).to_csv('optimizer_best.csv', index=False)
        print(f"\n★ 최고조건 저장: optimizer_best.csv")

    if decent_results:
        pd.DataFrame(decent_results).to_csv('optimizer_decent.csv', index=False)
        print(f"◎ 괜찮은조건 저장: optimizer_decent.csv")

    if all_summary:
        df_all = pd.DataFrame(all_summary)
        df_all = df_all.sort_values('win_rate', ascending=False).head(100)
        df_all.to_csv('optimizer_all_top100.csv', index=False)
        print(f"전체 상위100 저장: optimizer_all_top100.csv")

    print(f"\n{'='*80}")
    print(f"  탐색 완료!")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
