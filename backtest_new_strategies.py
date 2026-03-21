#!/usr/bin/env python3
"""
강화 전략 F1~F7 백테스트
- 기존 A~E 전략을 기반으로 승률 + 수익률 모두 높은 조합 탐색
- 유니버스: S&P500 + 소형주 샘플 (~600종목)
- 기간: 최근 2년
- 각 전략별 신호 발생 빈도, 승률, 평균/중간 수익률, MDD 측정
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ─── 유니버스 ─────────────────────────────────────────────────────────────────

def get_sp500_tickers():
    """S&P500 종목 가져오기"""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        df = tables[0]
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        return tickers
    except:
        return ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','JPM','V','JNJ',
                'WMT','PG','MA','UNH','HD','DIS','BAC','NFLX','ADBE','CRM',
                'PYPL','INTC','AMD','CSCO','PEP','KO','MRK','ABT','TMO','AVGO',
                'COST','NKE','ACN','LLY','ORCL','TXN','QCOM','LOW','UPS','BA',
                'GS','BLK','ISRG','MDLZ','ADP','GILD','AMGN','MO','SBUX','CAT']

def get_extra_smallcap():
    """추가 소형/중형주 + 변동성 큰 종목"""
    return ['SOFI','PLTR','RIVN','LCID','NIO','SNAP','PINS','ROKU','CRWD','DDOG',
            'NET','RBLX','HOOD','COIN','MARA','RIOT','UPST','AFRM','GSAT','BB',
            'NOK','SNDL','TLRY','CGC','ACB','PLUG','FCEL','BLNK','CHPT','QS',
            'NKLA','WKHS','DKNG','PENN','MGNI','FUBO','OPEN','LMND','ROOT',
            'SMCI','IONQ','RGTI','QUBT','KULR','OKLO','LUNR','RKLB','ASTS','APLD',
            'MSTR','SOS','SOXL','TQQQ','SQQQ','LABU','SPXS',
            'AMC','GME','BBBY','EXPR','CLOV','WISH','SKLZ','GOEV','MULN',
            'PHUN','DWAC','BKKT','IRNT','ATER','PROG','XELA','BBIG']

# ─── 지표 계산 ─────────────────────────────────────────────────────────────────

def calc_rsi(series, period):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def consecutive_down_days(close):
    """연속 하락일수 시리즈"""
    is_down = close < close.shift(1)
    groups = (~is_down).cumsum()
    return is_down.groupby(groups).cumsum().astype(int)

# ─── 전략 F1: 초극단 과매도 반등 (A 강화) ────────────────────────────────────
# RSI7 < 10 + 일중변동 > 25% + 3일수익률 < -20% + 연속하락 > 7일 + 5일저점거리 < 3%
# TP: +30% / SL: -10% / 보유: 15일

def strategy_F1(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)

    rsi7 = calc_rsi(close, 7)
    intra_range = (high - low) / low * 100
    ret_3d = close.pct_change(3) * 100
    down_days = consecutive_down_days(close)
    low_5d = low.rolling(5).min()
    dist_low5 = (close - low_5d) / low_5d * 100

    for i in range(20, len(df)):
        if pd.isna(rsi7.iloc[i]) or pd.isna(ret_3d.iloc[i]):
            continue
        if (rsi7.iloc[i] < 10 and
            intra_range.iloc[i] > 25 and
            ret_3d.iloc[i] < -20 and
            down_days.iloc[i] > 7 and
            not pd.isna(dist_low5.iloc[i]) and dist_low5.iloc[i] < 3):
            signals.append({
                'entry_date': df.index[i],
                'entry_price': float(close.iloc[i]),
                'tp': 0.30, 'sl': -0.10, 'max_hold': 15,
            })
    return signals

# ─── 전략 F2: 폭락주 대형반등 (D 확장) ──────────────────────────────────────
# 가격 <= $5 + 5일수익률 <= -50% + 일중변동 >= 30% + RSI14 <= 20
# TP: +50% / SL: -15% / 보유: 20일

def strategy_F2(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)

    rsi14 = calc_rsi(close, 14)
    intra_range = (high - low) / low * 100
    ret_5d = close.pct_change(5) * 100

    for i in range(20, len(df)):
        c = float(close.iloc[i])
        if pd.isna(rsi14.iloc[i]) or pd.isna(ret_5d.iloc[i]):
            continue
        if (c <= 5 and
            ret_5d.iloc[i] <= -50 and
            intra_range.iloc[i] >= 30 and
            rsi14.iloc[i] <= 20):
            signals.append({
                'entry_date': df.index[i],
                'entry_price': c,
                'tp': 0.50, 'sl': -0.15, 'max_hold': 20,
            })
    return signals

# ─── 전략 F3: 거래량폭증 과매도 (C+E 융합) ──────────────────────────────────
# RSI7 < 15 + 일중변동 > 20% + 거래량 >= 5일평균 5배 + 1일수익률 < -10%
# TP: +30% / SL: -10% / 보유: 10일

def strategy_F3(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    vol = df['Volume'].astype(float)

    rsi7 = calc_rsi(close, 7)
    intra_range = (high - low) / low * 100
    ret_1d = close.pct_change(1) * 100
    vol_ma5 = vol.rolling(5).mean()

    for i in range(20, len(df)):
        if pd.isna(rsi7.iloc[i]) or pd.isna(ret_1d.iloc[i]) or pd.isna(vol_ma5.iloc[i]):
            continue
        if (rsi7.iloc[i] < 15 and
            intra_range.iloc[i] > 20 and
            vol.iloc[i] >= vol_ma5.iloc[i] * 5 and
            ret_1d.iloc[i] < -10):
            signals.append({
                'entry_date': df.index[i],
                'entry_price': float(close.iloc[i]),
                'tp': 0.30, 'sl': -0.10, 'max_hold': 10,
            })
    return signals

# ─── 전략 F4: 저가주 극한반등 (D+E 강화) ────────────────────────────────────
# $1~$10 + 5일수익률 <= -35% + 일중변동 >= 25% + 연속하락 >= 5 + 거래량 >= 500K
# TP: +40% / SL: -12% / 보유: 15일

def strategy_F4(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    vol = df['Volume'].astype(float)

    intra_range = (high - low) / low * 100
    ret_5d = close.pct_change(5) * 100
    down_days = consecutive_down_days(close)

    for i in range(20, len(df)):
        c = float(close.iloc[i])
        if pd.isna(ret_5d.iloc[i]):
            continue
        if (1 <= c <= 10 and
            ret_5d.iloc[i] <= -35 and
            intra_range.iloc[i] >= 25 and
            down_days.iloc[i] >= 5 and
            vol.iloc[i] >= 500_000):
            signals.append({
                'entry_date': df.index[i],
                'entry_price': c,
                'tp': 0.40, 'sl': -0.12, 'max_hold': 15,
            })
    return signals

# ─── 전략 F5: 멀티시그널 A+C 동시 (복합) ────────────────────────────────────
# A조건: RSI7<20, 일중>20%, 3일수익률<-15%, 연속>5, 저점거리<5%
# + C조건: RSI7<30, 1일수익률<-8%, 전일하락, 연속>3, 저점거리<3%
# → 둘 다 만족 시 초강력 신호
# TP: +30% / SL: -8% / 보유: 10일

def strategy_F5(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)

    rsi7 = calc_rsi(close, 7)
    intra_range = (high - low) / low * 100
    ret_3d = close.pct_change(3) * 100
    ret_1d = close.pct_change(1) * 100
    down_days = consecutive_down_days(close)
    low_5d = low.rolling(5).min()
    dist_low5 = (close - low_5d) / low_5d * 100

    for i in range(20, len(df)):
        if any(pd.isna(x.iloc[i]) for x in [rsi7, ret_3d, ret_1d, dist_low5]):
            continue
        # A조건
        cond_a = (rsi7.iloc[i] < 20 and
                  intra_range.iloc[i] > 20 and
                  ret_3d.iloc[i] < -15 and
                  down_days.iloc[i] > 5 and
                  dist_low5.iloc[i] < 5)
        # C조건
        prev_down = (i > 0 and close.iloc[i-1] < close.iloc[i-2]) if i >= 2 else False
        cond_c = (rsi7.iloc[i] < 30 and
                  ret_1d.iloc[i] < -8 and
                  prev_down and
                  down_days.iloc[i] > 3 and
                  dist_low5.iloc[i] < 3)
        if cond_a and cond_c:
            signals.append({
                'entry_date': df.index[i],
                'entry_price': float(close.iloc[i]),
                'tp': 0.30, 'sl': -0.08, 'max_hold': 10,
            })
    return signals

# ─── 전략 F6: 기존A 확장 (TP 30%, 보유 20일) ────────────────────────────────
# 기존A 조건 그대로: RSI7<20, 일중>20%, 3일수익률<-15%, 연속>5, 저점거리<5%
# TP만 5% → 30%, 보유 5일 → 20일로 확대

def strategy_F6(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)

    rsi7 = calc_rsi(close, 7)
    intra_range = (high - low) / low * 100
    ret_3d = close.pct_change(3) * 100
    down_days = consecutive_down_days(close)
    low_5d = low.rolling(5).min()
    dist_low5 = (close - low_5d) / low_5d * 100

    for i in range(20, len(df)):
        if pd.isna(rsi7.iloc[i]) or pd.isna(ret_3d.iloc[i]) or pd.isna(dist_low5.iloc[i]):
            continue
        if (rsi7.iloc[i] < 20 and
            intra_range.iloc[i] > 20 and
            ret_3d.iloc[i] < -15 and
            down_days.iloc[i] > 5 and
            dist_low5.iloc[i] < 5):
            signals.append({
                'entry_date': df.index[i],
                'entry_price': float(close.iloc[i]),
                'tp': 0.30, 'sl': -0.10, 'max_hold': 20,
            })
    return signals

# ─── 전략 F7: 기존E 확장 (TP 30%, 보유 30일) ────────────────────────────────
# 기존E 조건: $3~$10, 5일수익률<=-25%, 일중>=20%, 연속>=5, 거래량>=200K
# TP만 10% → 30%, 보유 30일 유지

def strategy_F7(df):
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    vol = df['Volume'].astype(float)

    intra_range = (high - low) / low * 100
    ret_5d = close.pct_change(5) * 100
    down_days = consecutive_down_days(close)

    for i in range(20, len(df)):
        c = float(close.iloc[i])
        if pd.isna(ret_5d.iloc[i]):
            continue
        if (3 <= c <= 10 and
            ret_5d.iloc[i] <= -25 and
            intra_range.iloc[i] >= 20 and
            down_days.iloc[i] >= 5 and
            vol.iloc[i] >= 200_000):
            signals.append({
                'entry_date': df.index[i],
                'entry_price': c,
                'tp': 0.30, 'sl': -0.10, 'max_hold': 30,
            })
    return signals

# ─── 백테스트 엔진 ─────────────────────────────────────────────────────────────

def backtest_signal(df, signal):
    """단일 신호의 결과 시뮬레이션 (D+1 진입 가정)"""
    entry_date = signal['entry_date']
    entry_price = signal['entry_price']
    tp_pct = signal['tp']
    sl_pct = signal['sl']
    max_hold = signal['max_hold']

    future = df[df.index > entry_date]
    if len(future) == 0:
        return None

    tp_price = entry_price * (1 + tp_pct)
    sl_price = entry_price * (1 + sl_pct) if sl_pct else 0
    max_price = entry_price

    for day_idx, (date, row) in enumerate(future.iterrows()):
        if day_idx >= max_hold:
            break

        h = float(row['High'])
        l = float(row['Low'])
        c = float(row['Close'])
        max_price = max(max_price, h)

        if h >= tp_price:
            return {
                'result': 'WIN',
                'pct': tp_pct * 100,
                'days': day_idx + 1,
                'max_gain': (max_price - entry_price) / entry_price * 100,
            }

        if sl_price > 0 and l <= sl_price:
            return {
                'result': 'LOSS',
                'pct': sl_pct * 100,
                'days': day_idx + 1,
                'max_gain': (max_price - entry_price) / entry_price * 100,
            }

    # 타임아웃
    if len(future) > 0:
        last_idx = min(max_hold - 1, len(future) - 1)
        exit_price = float(future.iloc[last_idx]['Close'])
        exit_pct = (exit_price - entry_price) / entry_price * 100
        return {
            'result': 'WIN' if exit_pct > 0 else 'EXPIRED',
            'pct': exit_pct,
            'days': last_idx + 1,
            'max_gain': (max_price - entry_price) / entry_price * 100,
        }
    return None

# ─── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("  강화 전략 F1~F7 백테스트 (기존 A~E 기반)")
    print("  기간: 최근 2년 | 유니버스: S&P500 + 소형주 + 변동주")
    print("=" * 80)

    tickers = get_sp500_tickers() + get_extra_smallcap()
    tickers = sorted(set(tickers))
    print(f"\n총 {len(tickers)}개 종목 대상")

    strategies = {
        'F1': {'name': '초극단 과매도 +30% (A강화)', 'func': strategy_F1, 'signals': [], 'results': []},
        'F2': {'name': '폭락주 대형반등 +50% (D확장)', 'func': strategy_F2, 'signals': [], 'results': []},
        'F3': {'name': '거래량폭증 과매도 +30% (C+E)', 'func': strategy_F3, 'signals': [], 'results': []},
        'F4': {'name': '저가주 극한반등 +40% (D+E)', 'func': strategy_F4, 'signals': [], 'results': []},
        'F5': {'name': '멀티시그널 A+C +30% (복합)', 'func': strategy_F5, 'signals': [], 'results': []},
        'F6': {'name': '기존A→TP30% (20일보유)', 'func': strategy_F6, 'signals': [], 'results': []},
        'F7': {'name': '기존E→TP30% (30일보유)', 'func': strategy_F7, 'signals': [], 'results': []},
    }

    batch_size = 20
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for b_idx in range(0, len(tickers), batch_size):
        batch = tickers[b_idx:b_idx + batch_size]
        batch_num = b_idx // batch_size + 1

        if batch_num % 10 == 1 or batch_num == total_batches:
            sig_counts = ' | '.join(f"{k}:{len(v['signals'])}" for k, v in strategies.items())
            print(f"  Batch {batch_num}/{total_batches} | 신호: {sig_counts}")

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
                if len(df) < 50:
                    continue

                for key, strat in strategies.items():
                    sigs = strat['func'](df)
                    for sig in sigs:
                        sig['ticker'] = tk
                        result = backtest_signal(df, sig)
                        if result:
                            result['ticker'] = tk
                            result['entry_date'] = sig['entry_date']
                            result['entry_price'] = sig['entry_price']
                            strat['signals'].append(sig)
                            strat['results'].append(result)
            except:
                continue

        time.sleep(0.5)

    # ─── 결과 출력 ──────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("  백테스트 결과 요약")
    print("=" * 80)

    summary_rows = []

    for key, strat in strategies.items():
        results = strat['results']
        n = len(results)

        if n == 0:
            print(f"\n[{key}] {strat['name']}: 신호 없음")
            summary_rows.append({
                'strategy': key, 'name': strat['name'],
                'signals': 0, 'win_rate': 0, 'avg_pct': 0,
                'median_pct': 0, 'max_gain': 0, 'max_loss': 0,
                'avg_days': 0, 'monthly_freq': 0,
            })
            continue

        wins = [r for r in results if r['result'] == 'WIN']
        losses = [r for r in results if r['result'] == 'LOSS']
        expired = [r for r in results if r['result'] == 'EXPIRED']

        pcts = [r['pct'] for r in results]
        win_rate = len(wins) / n * 100
        avg_pct = np.mean(pcts)
        median_pct = np.median(pcts)
        max_gain = max(r['max_gain'] for r in results)
        max_loss = min(pcts)
        avg_days = np.mean([r['days'] for r in results])

        if results:
            dates = [r['entry_date'] for r in results]
            date_range_days = (max(dates) - min(dates)).days
            months = max(date_range_days / 30, 1)
            monthly_freq = n / months
        else:
            monthly_freq = 0

        summary_rows.append({
            'strategy': key, 'name': strat['name'],
            'signals': n, 'win_rate': win_rate, 'avg_pct': avg_pct,
            'median_pct': median_pct, 'max_gain': max_gain, 'max_loss': max_loss,
            'avg_days': avg_days, 'monthly_freq': monthly_freq,
        })

        print(f"\n{'─'*70}")
        print(f"  [{key}] {strat['name']}")
        print(f"{'─'*70}")
        print(f"  총 신호:    {n}건 (월 평균 {monthly_freq:.1f}건)")
        print(f"  승률:       {win_rate:.1f}% ({len(wins)}승 / {len(losses)}패 / {len(expired)}만기)")
        print(f"  평균 수익:  {avg_pct:+.1f}% | 중간값: {median_pct:+.1f}%")
        print(f"  최대 수익:  {max_gain:+.1f}% | 최대 손실: {max_loss:+.1f}%")
        print(f"  평균 보유:  {avg_days:.1f}일")

        top5 = sorted(results, key=lambda x: x['pct'], reverse=True)[:5]
        print(f"  상위 5건:")
        for r in top5:
            d = r['entry_date'].strftime('%Y-%m-%d') if hasattr(r['entry_date'], 'strftime') else str(r['entry_date'])
            print(f"    {r['ticker']:6s} | {d} | ${r['entry_price']:.2f} → {r['pct']:+.1f}% ({r['days']}일)")

    # 비교 테이블
    print(f"\n{'='*100}")
    print(f"  전략 비교 요약 (승률 + 수익률 기준)")
    print(f"{'='*100}")
    print(f"{'전략':^6} {'이름':^28} {'신호수':>6} {'월빈도':>7} {'승률':>7} {'평균%':>8} {'중간%':>8} {'최대↑':>8} {'최대↓':>8}")
    print("-" * 100)
    for s in summary_rows:
        print(f"  {s['strategy']:^4} {s['name']:^28} {s['signals']:>6} {s['monthly_freq']:>6.1f} "
              f"{s['win_rate']:>6.1f}% {s['avg_pct']:>+7.1f}% {s['median_pct']:>+7.1f}% "
              f"{s['max_gain']:>+7.1f}% {s['max_loss']:>+7.1f}%")

    # 추천 전략 하이라이트
    valid = [s for s in summary_rows if s['signals'] >= 3]
    if valid:
        print(f"\n{'='*80}")
        print("  ★ 추천 전략 (신호 3건 이상 중 승률×평균수익 기준)")
        print(f"{'='*80}")
        ranked = sorted(valid, key=lambda x: x['win_rate'] * max(x['avg_pct'], 0.01), reverse=True)
        for i, s in enumerate(ranked[:3]):
            score = s['win_rate'] * max(s['avg_pct'], 0.01)
            print(f"  {i+1}위: [{s['strategy']}] {s['name']}")
            print(f"       승률 {s['win_rate']:.1f}% × 평균수익 {s['avg_pct']:+.1f}% = 점수 {score:.0f}")
            print(f"       신호 {s['signals']}건 | 월 {s['monthly_freq']:.1f}건\n")

    # CSV 저장
    if summary_rows:
        pd.DataFrame(summary_rows).to_csv('backtest_results.csv', index=False)
        print(f"\n결과 저장: backtest_results.csv")

    # 상세 거래내역 저장
    all_trades = []
    for key, strat in strategies.items():
        for r in strat['results']:
            r_copy = r.copy()
            r_copy['strategy'] = key
            all_trades.append(r_copy)
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_df.to_csv('backtest_trades.csv', index=False)
        print(f"상세 거래내역 저장: backtest_trades.csv")

if __name__ == '__main__':
    main()
