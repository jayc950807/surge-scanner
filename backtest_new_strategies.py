#!/usr/bin/env python3
"""
신규 전략 F/G/H/I 백테스트
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
        # 폴백: 대표 종목
        return ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','JPM','V','JNJ',
                'WMT','PG','MA','UNH','HD','DIS','BAC','NFLX','ADBE','CRM',
                'PYPL','INTC','AMD','CSCO','PEP','KO','MRK','ABT','TMO','AVGO',
                'COST','NKE','ACN','LLY','ORCL','TXN','QCOM','LOW','UPS','BA',
                'GS','BLK','ISRG','MDLZ','ADP','GILD','AMGN','MO','SBUX','CAT']

def get_extra_smallcap():
    """추가 소형/중형주 샘플"""
    return ['SOFI','PLTR','RIVN','LCID','NIO','SNAP','PINS','ROKU','CRWD','DDOG',
            'NET','RBLX','HOOD','COIN','MARA','RIOT','UPST','AFRM','SKLZ','WISH',
            'CLOV','BNGO','SENS','GSAT','BB','NOK','SNDL','TLRY','CGC','ACB',
            'PLUG','FCEL','BLNK','CHPT','QS','GOEV','FSR','NKLA','HYLN','WKHS',
            'SPCE','DKNG','PENN','MGNI','FUBO','TTCF','OPEN','LMND','ROOT','VROOM']

# ─── 지표 계산 ─────────────────────────────────────────────────────────────────

def calc_rsi(series, period):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_bb(close, period=20, std_mult=2):
    """볼린저 밴드"""
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = ma + std_mult * std
    lower = ma - std_mult * std
    width = (upper - lower) / ma  # 밴드 폭 비율
    return ma, upper, lower, width

# ─── 전략 F: 볼린저 스퀴즈 돌파 ────────────────────────────────────────────────

def strategy_F(df):
    """
    진입: BB 폭 120일 최저 + 종가 > BB 상단 + 거래량 >= 20일 평균 2배
    청산: +30% TP / -10% SL / 20일 타임아웃
    """
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    vol = df['Volume'].astype(float)

    _, upper, _, width = calc_bb(close, 20, 2)
    vol_ma20 = vol.rolling(20).mean()
    width_min120 = width.rolling(120).min()

    for i in range(121, len(df)):
        if pd.isna(width.iloc[i]) or pd.isna(width_min120.iloc[i]):
            continue
        # BB 폭이 120일 최저 수준 (최저값의 1.05배 이내)
        if width.iloc[i] <= width_min120.iloc[i] * 1.05:
            # 종가가 상단밴드 돌파
            if close.iloc[i] > upper.iloc[i]:
                # 거래량 >= 20일 평균 2배
                if not pd.isna(vol_ma20.iloc[i]) and vol.iloc[i] >= vol_ma20.iloc[i] * 2:
                    entry_price = float(close.iloc[i])
                    entry_date = df.index[i]
                    signals.append({
                        'entry_date': entry_date,
                        'entry_price': entry_price,
                        'tp': 0.30,
                        'sl': -0.10,
                        'max_hold': 20,
                    })
    return signals

# ─── 전략 G: 갭업 후 눌림 재돌파 ─────────────────────────────────────────────

def strategy_G(df):
    """
    진입: 5일 내 +5% 갭업 이력 + 이후 조정(-3%~-8%) + 갭업일 종가 돌파 + 거래량 확인
    청산: +35% TP / -10% SL / 20일 타임아웃
    """
    signals = []
    close = df['Close'].astype(float)
    opn = df['Open'].astype(float)
    vol = df['Volume'].astype(float)
    vol_ma20 = vol.rolling(20).mean()

    for i in range(10, len(df)):
        # 최근 5일 내 갭업(시가 > 전일 종가 +5%) 찾기
        gap_day = None
        gap_high = None
        for j in range(i-5, i):
            if j <= 0:
                continue
            gap_pct = (opn.iloc[j] - close.iloc[j-1]) / close.iloc[j-1]
            if gap_pct >= 0.05:
                gap_day = j
                gap_high = float(close.iloc[j])
                break

        if gap_day is None or gap_high is None:
            continue

        # 갭업 이후 조정 확인: 고점 대비 -3% ~ -8% 눌림
        post_gap_high = float(close.iloc[gap_day:i].max())
        current_pullback = (close.iloc[i-1] - post_gap_high) / post_gap_high

        if not (-0.08 <= current_pullback <= -0.03):
            continue

        # 오늘 갭업일 종가 돌파 + 거래량
        if close.iloc[i] > gap_high:
            if not pd.isna(vol_ma20.iloc[i]) and vol.iloc[i] >= vol_ma20.iloc[i] * 1.5:
                entry_price = float(close.iloc[i])
                signals.append({
                    'entry_date': df.index[i],
                    'entry_price': entry_price,
                    'tp': 0.35,
                    'sl': -0.10,
                    'max_hold': 20,
                })
    return signals

# ─── 전략 H: 거래량 폭발 저가 반등 ───────────────────────────────────────────

def strategy_H(df):
    """
    진입: 50MA 대비 -30% 이하 + 거래량 >= 50일 평균 10배 + 양봉 + 종가 $5~$30
    청산: +40% TP / -15% SL / 30일 타임아웃
    """
    signals = []
    close = df['Close'].astype(float)
    opn = df['Open'].astype(float)
    vol = df['Volume'].astype(float)

    ma50 = close.rolling(50).mean()
    vol_ma50 = vol.rolling(50).mean()

    for i in range(50, len(df)):
        c = float(close.iloc[i])
        o = float(opn.iloc[i])

        if c < 5 or c > 30:
            continue
        if pd.isna(ma50.iloc[i]) or pd.isna(vol_ma50.iloc[i]):
            continue

        # 50MA 대비 -30% 이하
        ma50_dist = (c - float(ma50.iloc[i])) / float(ma50.iloc[i])
        if ma50_dist > -0.30:
            continue

        # 거래량 >= 50일 평균 10배
        if vol.iloc[i] < vol_ma50.iloc[i] * 10:
            continue

        # 양봉
        if c <= o:
            continue

        signals.append({
            'entry_date': df.index[i],
            'entry_price': c,
            'tp': 0.40,
            'sl': -0.15,
            'max_hold': 30,
        })
    return signals

# ─── 전략 I: 다중 MA 모멘텀 ──────────────────────────────────────────────────

def strategy_I(df):
    """
    진입: 5MA > 20MA > 50MA + RSI 50~65 + 거래량 >= 20일 평균 1.5배 + 종가 > 전일 고가
    청산: +30% TP / -8% SL / 15일 타임아웃
    """
    signals = []
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    vol = df['Volume'].astype(float)

    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()
    rsi14 = calc_rsi(close, 14)
    vol_ma20 = vol.rolling(20).mean()

    for i in range(51, len(df)):
        if any(pd.isna(x.iloc[i]) for x in [ma5, ma20, ma50, rsi14, vol_ma20]):
            continue

        # 정배열: 5MA > 20MA > 50MA
        if not (ma5.iloc[i] > ma20.iloc[i] > ma50.iloc[i]):
            continue

        # RSI 50~65
        rsi_val = float(rsi14.iloc[i])
        if not (50 <= rsi_val <= 65):
            continue

        # 거래량 >= 20일 평균 1.5배
        if vol.iloc[i] < vol_ma20.iloc[i] * 1.5:
            continue

        # 종가 > 전일 고가
        if close.iloc[i] <= high.iloc[i-1]:
            continue

        signals.append({
            'entry_date': df.index[i],
            'entry_price': float(close.iloc[i]),
            'tp': 0.30,
            'sl': -0.08,
            'max_hold': 15,
        })
    return signals

# ─── 백테스트 엔진 ─────────────────────────────────────────────────────────────

def backtest_signal(df, signal):
    """단일 신호의 결과 시뮬레이션"""
    entry_date = signal['entry_date']
    entry_price = signal['entry_price']
    tp_pct = signal['tp']
    sl_pct = signal['sl']
    max_hold = signal['max_hold']

    # entry_date 이후의 데이터 (D+1부터 매수 가정)
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

        # TP 체크 (일중 고가 기준)
        if h >= tp_price:
            return {
                'result': 'WIN',
                'pct': tp_pct * 100,
                'days': day_idx + 1,
                'max_gain': (max_price - entry_price) / entry_price * 100,
            }

        # SL 체크
        if sl_price > 0 and l <= sl_price:
            return {
                'result': 'LOSS',
                'pct': sl_pct * 100,
                'days': day_idx + 1,
                'max_gain': (max_price - entry_price) / entry_price * 100,
            }

    # 타임아웃 — 마지막 종가 기준 청산
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
    print("  신규 전략 F/G/H/I 백테스트")
    print("  기간: 최근 2년 | 유니버스: S&P500 + 소형주 샘플")
    print("=" * 80)

    # 종목 수집
    tickers = get_sp500_tickers() + get_extra_smallcap()
    tickers = sorted(set(tickers))
    print(f"\n총 {len(tickers)}개 종목 대상")

    strategies = {
        'F': {'name': '볼린저 스퀴즈 돌파 +30%', 'func': strategy_F, 'signals': [], 'results': []},
        'G': {'name': '갭업 눌림 재돌파 +35%', 'func': strategy_G, 'signals': [], 'results': []},
        'H': {'name': '거래량 폭발 저가 반등 +40%', 'func': strategy_H, 'signals': [], 'results': []},
        'I': {'name': '다중 MA 모멘텀 +30%', 'func': strategy_I, 'signals': [], 'results': []},
    }

    # 배치 다운로드 + 전략 스캔
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
                        # yfinance 최신: (Price, Ticker) 또는 (Ticker, Price)
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
                if len(df) < 130:  # 최소 120일 + 여유
                    continue

                # 각 전략 스캔
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

        # 월 평균 신호 빈도
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

        # 상위 5개 수익 종목
        top5 = sorted(results, key=lambda x: x['pct'], reverse=True)[:5]
        print(f"  상위 5건:")
        for r in top5:
            d = r['entry_date'].strftime('%Y-%m-%d') if hasattr(r['entry_date'], 'strftime') else str(r['entry_date'])
            print(f"    {r['ticker']:6s} | {d} | ${r['entry_price']:.2f} → {r['pct']:+.1f}% ({r['days']}일)")

    # 비교 테이블
    print(f"\n{'='*90}")
    print(f"  전략 비교 요약")
    print(f"{'='*90}")
    print(f"{'전략':^6} {'이름':^24} {'신호수':>6} {'월빈도':>7} {'승률':>7} {'평균%':>8} {'중간%':>8} {'최대↑':>8} {'최대↓':>8}")
    print("-" * 90)
    for s in summary_rows:
        print(f"  {s['strategy']:^4} {s['name']:^24} {s['signals']:>6} {s['monthly_freq']:>6.1f} "
              f"{s['win_rate']:>6.1f}% {s['avg_pct']:>+7.1f}% {s['median_pct']:>+7.1f}% "
              f"{s['max_gain']:>+7.1f}% {s['max_loss']:>+7.1f}%")

    # CSV 저장
    if summary_rows:
        pd.DataFrame(summary_rows).to_csv('/sessions/sleepy-friendly-darwin/mnt/outputs/backtest_results.csv', index=False)
        print(f"\n결과 저장: backtest_results.csv")

if __name__ == '__main__':
    main()
