"""
전략 F/G 정밀도 검증
====================
30일 사전 윈도우 패턴이 나타났을 때, 이후 실제로 급등한 비율을 측정.

전략 F (급락반등 사전탐지):
  D1: 60일고점 대비 -20% 이하 + RSI(14) < 50 + SMA50 하회
  D2: 30일 추세기울기 < -0.3%/일 + ATR > 10% + 하락일 비율 > 15%
  D3: 30일 수익률 < -15% + 60일고점 대비 -30% + RSI(14) < 45

전략 G (모멘텀폭발 사전탐지):
  W1: SMA20 대비 +20% 이상 + SMA50 상회 + RSI(14) > 60
  W2: 30일 수익률 > 50% + 60일고점 대비 -15% 이내 + RSI > 70
  W3: 최대거래량 10배 이상 + 추세기울기 > 0.5%/일 + ATR > 10%

검증: 10,000 종목 × 5년. 신호 발생 → 이후 5/10/20일 내 최고가 수익률 측정.
"""

import os
import sys
import time
import random
import warnings
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ============================================================
# 설정
# ============================================================
SAMPLE_SIZE = int(os.environ.get("SAMPLE_SIZE", "10000"))
LOOKBACK_YEARS = 5
MIN_PRICE = 1.0
COOLDOWN = 10
MAX_REASONABLE_RET = 1500

OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)


# ============================================================
# 종목 리스트 (기존과 동일)
# ============================================================
def get_ticker_universe(n=10000):
    tickers = set()
    try:
        sp = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        tickers.update(sp["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist())
    except Exception as e:
        print(f"[WARN] SP500 fetch failed: {e}")
    try:
        df = pd.read_csv("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt", sep="|")
        df = df[df["Test Issue"] == "N"]
        tickers.update(df["Symbol"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] NASDAQ listed fetch failed: {e}")
    try:
        df = pd.read_csv("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt", sep="|")
        df = df[df["Test Issue"] == "N"]
        tickers.update(df["ACT Symbol"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Other listed fetch failed: {e}")

    clean = []
    for t in tickers:
        if not isinstance(t, str):
            continue
        t = t.strip().upper()
        if not t or t == "NAN":
            continue
        if any(ch in t for ch in [" ", "$", "^", "/"]):
            continue
        clean.append(t.replace(".", "-"))
    clean = sorted(set(clean))
    random.seed(42)
    random.shuffle(clean)
    return clean[:n]


# ============================================================
# RSI (Wilder)
# ============================================================
def rsi_wilder(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_dn = dn.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_dn.replace(0, np.nan))
    return 100 - (100 / (1 + rs))


# ============================================================
# 지표 계산 (30일 윈도우 기반)
# ============================================================
def add_indicators(df):
    c = df["Close"]
    h = df["High"]
    l = df["Low"]
    v = df["Volume"]

    # 기본 이평
    df["sma20"] = c.rolling(20).mean()
    df["sma50"] = c.rolling(50).mean()
    df["vol20"] = v.rolling(20).mean()
    df["rsi14"] = rsi_wilder(c, 14)

    # 60일 고점
    df["high60"] = h.rolling(60).max()

    # 60일고점 대비 거리 (%)
    df["dist_high60"] = (c / df["high60"] - 1) * 100

    # SMA50 대비 거리 (%)
    df["dist_sma50"] = (c / df["sma50"] - 1) * 100

    # SMA20 대비 거리 (%)
    df["dist_sma20"] = (c / df["sma20"] - 1) * 100

    # 30일 수익률
    df["ret_30d"] = c.pct_change(30) * 100

    # 30일 추세 기울기 (%/일) — 선형회귀 대신 단순 계산 (시작 vs 끝)
    df["trend_slope"] = df["ret_30d"] / 30

    # ATR (14일)
    tr = pd.concat([
        h - l,
        (h - c.shift(1)).abs(),
        (l - c.shift(1)).abs(),
    ], axis=1).max(axis=1)
    df["atr14"] = tr.rolling(14).mean()
    df["atr_pct"] = (df["atr14"] / c) * 100

    # 30일 내 하락일(>3%) 비율
    daily_ret = c.pct_change() * 100
    df["daily_ret"] = daily_ret
    df["big_down_30"] = daily_ret.rolling(30).apply(lambda x: (x < -3).sum() / 30 * 100, raw=True)

    # 30일 내 상승일(>3%) 비율
    df["big_up_30"] = daily_ret.rolling(30).apply(lambda x: (x > 3).sum() / 30 * 100, raw=True)

    # 30일 내 최대 거래량 / 20일 평균
    df["max_vol_ratio_30"] = v.rolling(30).max() / df["vol20"]

    # 30일 추세 기울기 (선형회귀)
    def linreg_slope(window):
        x = np.arange(len(window))
        if len(window) < 2 or np.std(window) == 0:
            return 0.0
        slope = np.polyfit(x, window, 1)[0]
        return slope / window.iloc[0] * 100 if window.iloc[0] != 0 else 0.0
    df["trend_slope_lr"] = c.rolling(30).apply(linreg_slope, raw=False)

    return df


# ============================================================
# 전략 F: 급락반등 사전탐지
# ============================================================
def check_strategy_F(row):
    """D1 OR D2 OR D3 중 하나 이상 충족"""
    c = row["Close"]
    rsi = row["rsi14"]
    dist_h60 = row["dist_high60"]
    dist_sma50 = row["dist_sma50"]
    trend = row["trend_slope_lr"]
    atr = row["atr_pct"]
    big_down = row["big_down_30"]
    ret30 = row["ret_30d"]

    required = [c, rsi, dist_h60, dist_sma50, trend, atr, big_down, ret30]
    if any(pd.isna(x) for x in required):
        return False

    if c < MIN_PRICE:
        return False

    # D1: 60일고점 -20% + RSI<50 + SMA50 하회
    d1 = (dist_h60 < -20) and (rsi < 50) and (dist_sma50 < 0)

    # D2: 추세기울기 < -0.3%/일 + ATR > 10% + 하락일 > 15%
    d2 = (trend < -0.3) and (atr > 10) and (big_down > 15)

    # D3: 30일수익률 < -15% + 60일고점 -30% + RSI < 45
    d3 = (ret30 < -15) and (dist_h60 < -30) and (rsi < 45)

    return d1 or d2 or d3


# ============================================================
# 전략 G: 모멘텀폭발 사전탐지
# ============================================================
def check_strategy_G(row):
    """W1 OR W2 OR W3 중 하나 이상 충족"""
    c = row["Close"]
    rsi = row["rsi14"]
    dist_sma20 = row["dist_sma20"]
    dist_sma50 = row["dist_sma50"]
    ret30 = row["ret_30d"]
    dist_h60 = row["dist_high60"]
    max_vol = row["max_vol_ratio_30"]
    trend = row["trend_slope_lr"]
    atr = row["atr_pct"]

    required = [c, rsi, dist_sma20, dist_sma50, ret30, dist_h60, max_vol, trend, atr]
    if any(pd.isna(x) for x in required):
        return False

    if c < MIN_PRICE:
        return False

    # W1: SMA20 +20% 이상 + SMA50 상회 + RSI > 60
    w1 = (dist_sma20 > 20) and (dist_sma50 > 0) and (rsi > 60)

    # W2: 30일수익률 > 50% + 60일고점 -15% 이내 + RSI > 70
    w2 = (ret30 > 50) and (dist_h60 > -15) and (rsi > 70)

    # W3: 최대거래량 10배 + 추세기울기 > 0.5%/일 + ATR > 10%
    w3 = (max_vol > 10) and (trend > 0.5) and (atr > 10)

    return w1 or w2 or w3


# ============================================================
# 종목 단위 처리
# ============================================================
def process_ticker(ticker, start, end):
    try:
        df = yf.download(ticker, start=start, end=end,
                         auto_adjust=True, progress=False, threads=False)
        if df is None or df.empty or len(df) < 80:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = add_indicators(df)
    except Exception:
        return None

    results = {"F": [], "G": []}
    last_signal = {"F": -10**9, "G": -10**9}

    for i in range(60, len(df) - 21):
        row = df.iloc[i]

        for strat, check_fn in [("F", check_strategy_F), ("G", check_strategy_G)]:
            if i - last_signal[strat] < COOLDOWN:
                continue
            if not check_fn(row):
                continue

            entry_idx = i + 1
            if entry_idx >= len(df):
                break
            entry = df["Open"].iloc[entry_idx]
            if pd.isna(entry) or entry <= 0:
                continue

            # 5/10/20일 내 최고가
            rets = {}
            for hold in [5, 10, 20]:
                end_idx = min(entry_idx + hold, len(df))
                if end_idx <= entry_idx:
                    continue
                window_high = df["High"].iloc[entry_idx:end_idx].max()
                window_low = df["Low"].iloc[entry_idx:end_idx].min()
                if pd.isna(window_high):
                    continue
                max_ret = (window_high / entry - 1) * 100
                max_dd = (window_low / entry - 1) * 100
                if max_ret > MAX_REASONABLE_RET or max_ret < -99:
                    max_ret = None
                rets[f"ret{hold}"] = max_ret
                rets[f"dd{hold}"] = max_dd

            if rets.get("ret5") is None:
                continue

            last_signal[strat] = i
            results[strat].append({
                "date": df.index[i].strftime("%Y-%m-%d"),
                "price": round(row["Close"], 2),
                "rsi": round(row["rsi14"], 1),
                **rets,
            })

    return {"ticker": ticker, "results": results}


# ============================================================
# 리포트 생성
# ============================================================
def build_report(strat_name, strat_label, all_signals, elapsed_min):
    lines = []
    lines.append(f"# 전략 {strat_name} 정밀도 검증 리포트")
    lines.append(f"# {strat_label}")
    lines.append("")
    lines.append(f"- 생성일: {datetime.now():%Y-%m-%d %H:%M}")
    lines.append(f"- 분석 기간: 최근 {LOOKBACK_YEARS}년")
    lines.append(f"- 신호 쿨다운: {COOLDOWN} 거래일 (동일 종목)")
    lines.append(f"- 소요 시간: {elapsed_min:.1f}분")
    lines.append("")

    n = len(all_signals)
    lines.append(f"## 전체 결과: 총 **{n:,}건** 신호")
    lines.append("")

    if n == 0:
        lines.append("신호 없음.")
        return "\n".join(lines)

    df = pd.DataFrame(all_signals)

    for hold in [5, 10, 20]:
        col = f"ret{hold}"
        dd_col = f"dd{hold}"
        if col not in df.columns:
            continue
        arr = df[col].dropna().values
        dd_arr = df[dd_col].dropna().values if dd_col in df.columns else np.array([])

        lines.append(f"### {hold}일 보유 기준")
        lines.append("")
        lines.append(f"- 평균 최대수익: {arr.mean():.2f}%")
        lines.append(f"- 중앙값: {np.median(arr):.2f}%")
        lines.append(f"- P25/P75: {np.percentile(arr,25):.2f}% / {np.percentile(arr,75):.2f}%")
        if len(dd_arr) > 0:
            lines.append(f"- 평균 최대손실: {dd_arr.mean():.2f}%")
            lines.append(f"- 최대 낙폭: {dd_arr.min():.2f}%")
        lines.append("")

        lines.append(f"| 타겟 ({hold}일 내 고가) | 도달 건수 | 정밀도 |")
        lines.append("|---|---:|---:|")
        for t in [5, 10, 15, 20, 30, 50, 75, 100]:
            hit = int((arr >= t).sum())
            pct = hit / len(arr) * 100
            lines.append(f"| +{t}% 이상 | {hit:,} | {pct:.2f}% |")
        lines.append("")

    # 기대값 분석
    lines.append("### 전략별 기대값 시뮬레이션")
    lines.append("")
    for tp, sl in [(10, -10), (15, -10), (20, -10), (20, -15), (30, -15), (30, -20)]:
        wins = 0
        losses = 0
        neutral = 0
        pnl_sum = 0.0
        for _, row in df.iterrows():
            # 20일 내에서 TP/SL 중 어느 것이 먼저 도달하는지 시뮬레이션
            # 간단 버전: ret20으로 TP 도달 여부, dd20으로 SL 도달 여부
            hit_tp = row.get("ret20", 0) is not None and row.get("ret20", 0) >= tp
            hit_sl = row.get("dd20", 0) is not None and row.get("dd20", 0) <= sl
            if hit_tp and not hit_sl:
                wins += 1
                pnl_sum += tp
            elif hit_sl and not hit_tp:
                losses += 1
                pnl_sum += sl
            elif hit_tp and hit_sl:
                # 둘 다 도달 → 50/50 가정 (보수적)
                wins += 0.5
                losses += 0.5
                pnl_sum += (tp + sl) / 2
            else:
                neutral += 1
                # 만기 시 중앙값 수익률 사용
                pnl_sum += min(max(row.get("ret20", 0) or 0, sl), tp)

        total = wins + losses + neutral
        win_rate = wins / total * 100 if total > 0 else 0
        avg_pnl = pnl_sum / total if total > 0 else 0
        lines.append(f"- TP +{tp}% / SL {sl}%: 승률 {win_rate:.1f}%, 평균 P&L {avg_pnl:+.2f}%, 건수 {int(total):,}")

    lines.append("")
    return "\n".join(lines)


# ============================================================
# 메인
# ============================================================
def main():
    t0 = time.time()
    tickers = get_ticker_universe(SAMPLE_SIZE)
    print(f"[INFO] Universe: {len(tickers)} tickers")
    end = datetime.today()
    start = end - timedelta(days=365 * LOOKBACK_YEARS + 90)

    all_F = []
    all_G = []

    for i, tkr in enumerate(tickers, 1):
        res = process_ticker(tkr, start, end)
        if res is None:
            continue

        for sig in res["results"]["F"]:
            sig["ticker"] = tkr
            all_F.append(sig)
        for sig in res["results"]["G"]:
            sig["ticker"] = tkr
            all_G.append(sig)

        if i % 100 == 0:
            elapsed = time.time() - t0
            eta = (elapsed / i) * (len(tickers) - i) / 60
            print(f"[{i}/{len(tickers)}] F={len(all_F)} G={len(all_G)} "
                  f"({elapsed/60:.1f}min, ETA {eta:.0f}min)")

    elapsed_min = (time.time() - t0) / 60

    # 리포트 생성
    report_f = build_report("F", "급락반등 사전탐지 (D1/D2/D3)", all_F, elapsed_min)
    report_g = build_report("G", "모멘텀폭발 사전탐지 (W1/W2/W3)", all_G, elapsed_min)

    with open(os.path.join(OUT_DIR, "precision_F.md"), "w", encoding="utf-8") as f:
        f.write(report_f)
    with open(os.path.join(OUT_DIR, "precision_G.md"), "w", encoding="utf-8") as f:
        f.write(report_g)

    # CSV 저장
    if all_F:
        pd.DataFrame(all_F).to_csv(os.path.join(OUT_DIR, "precision_F_signals.csv"), index=False)
    if all_G:
        pd.DataFrame(all_G).to_csv(os.path.join(OUT_DIR, "precision_G_signals.csv"), index=False)

    print(f"\n{'='*60}")
    print(f"[DONE] {elapsed_min:.1f}분 소요")
    print(f"전략 F: {len(all_F):,}건 신호")
    print(f"전략 G: {len(all_G):,}건 신호")
    print(f"리포트: data/precision_F.md, data/precision_G.md")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
