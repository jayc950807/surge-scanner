"""
Surge Precision Check
---------------------
surge_window_report.md 에서 도출한 5가지 공통 조건 필터의
'정밀도(precision)'를 측정한다.

필터 (5개 모두 AND):
    1) 종가 > SMA20
    2) 30일 최대 거래량 >= 20일 평균의 5배
    3) 이벤트 직전 RSI(14) >= 50
    4) 30일 누적 수익률 >= 0%
    5) SMA50 대비 +10% 이상 이격  (Close >= SMA50 * 1.10)

검증 방법:
- 10,000 종목을 5년치 일봉으로 훑는다.
- 매 거래일마다 5개 조건을 전부 만족하는지 확인한다.
- 만족한 '신호일(signal day)'이 나오면, 이후 5거래일 동안의
  High 최대값 대비 진입가(Open 다음날) 수익률을 계산한다.
- 두 타겟:
    A) 5일 내 고가 기준 +100% 도달률
    B) 5일 내 고가 기준 +50% 도달률
- 신호 중복 방지: 한 종목에서 신호가 나면, 이후 10거래일 동안 추가 신호 무시.

출력: data/precision_report.md (한글 텍스트)
"""

import os
import time
import math
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
MIN_PRICE = 2.0
MIN_DOLLAR_VOL = 500_000

HOLD_DAYS = 5            # 5거래일 이내
COOLDOWN = 10            # 신호 쿨다운(동일 종목)
MAX_REASONABLE_RET = 1500  # 1500% 초과는 이상치로 간주 (yfinance 분할 오류 등 방어)

OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)


# ============================================================
# 종목 리스트
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
# 지표
# ============================================================
def rsi_wilder(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_dn = dn.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_dn.replace(0, np.nan))
    return 100 - (100 / (1 + rs))


def add_indicators(df):
    c = df["Close"]
    v = df["Volume"]
    df["sma20"] = c.rolling(20).mean()
    df["sma50"] = c.rolling(50).mean()
    df["vol20"] = v.rolling(20).mean()
    df["rsi14"] = rsi_wilder(c, 14)
    df["dollar_vol"] = c * v
    # 30일 최대 거래량 (shift(1) 으로 당일 거래량 제외: look-ahead 방지)
    df["max_vol_30"] = v.rolling(30).max()
    # 30일 누적 수익률 (30일전 종가 대비 현재 종가)
    df["ret_30d"] = c.pct_change(30)
    return df


# ============================================================
# 필터 검사
# ============================================================
def passes_filter(row):
    c = row["Close"]
    sma20 = row["sma20"]
    sma50 = row["sma50"]
    rsi = row["rsi14"]
    max_vol_30 = row["max_vol_30"]
    vol20 = row["vol20"]
    ret30 = row["ret_30d"]
    dvol = row["dollar_vol"]

    if any(pd.isna(x) for x in [c, sma20, sma50, rsi, max_vol_30, vol20, ret30, dvol]):
        return False
    # 기본 유동성
    if c < MIN_PRICE or dvol < MIN_DOLLAR_VOL:
        return False
    # 1) Close > SMA20
    if c <= sma20:
        return False
    # 2) 30일 내 한 번이라도 거래량이 20일 평균의 5배 이상
    if vol20 <= 0:
        return False
    if (max_vol_30 / vol20) < 5.0:
        return False
    # 3) RSI >= 50
    if rsi < 50:
        return False
    # 4) 30일 누적 수익률 >= 0
    if ret30 < 0:
        return False
    # 5) Close >= SMA50 * 1.10
    if c < sma50 * 1.10:
        return False
    return True


# ============================================================
# 종목 단위 검사
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

    signals = 0
    hit50 = 0
    hit100 = 0
    max_rets = []
    last_signal_idx = -10**9

    # 마지막 HOLD_DAYS 만큼은 미래 데이터가 없으니 제외
    for i in range(50, len(df) - HOLD_DAYS - 1):
        if i - last_signal_idx < COOLDOWN:
            continue
        row = df.iloc[i]
        if not passes_filter(row):
            continue

        # 진입: 다음날 시가
        entry_idx = i + 1
        if entry_idx >= len(df):
            break
        entry = df["Open"].iloc[entry_idx]
        if pd.isna(entry) or entry <= 0:
            continue

        # 이후 HOLD_DAYS 동안의 최고가
        window_high = df["High"].iloc[entry_idx:entry_idx + HOLD_DAYS].max()
        if pd.isna(window_high):
            continue
        max_ret = (window_high / entry - 1) * 100

        # 이상치 가드 (분할/ data 오류)
        if max_ret > MAX_REASONABLE_RET or max_ret < -99:
            continue

        signals += 1
        last_signal_idx = i
        max_rets.append(max_ret)
        if max_ret >= 50:
            hit50 += 1
        if max_ret >= 100:
            hit100 += 1

    return {
        "ticker": ticker,
        "signals": signals,
        "hit50": hit50,
        "hit100": hit100,
        "max_rets": max_rets,
    }


# ============================================================
# 메인
# ============================================================
def main():
    t0 = time.time()
    tickers = get_ticker_universe(SAMPLE_SIZE)
    print(f"[INFO] Universe: {len(tickers)} tickers")
    end = datetime.today()
    start = end - timedelta(days=365 * LOOKBACK_YEARS + 30)

    total_signals = 0
    total_hit50 = 0
    total_hit100 = 0
    all_rets = []
    per_ticker_rows = []

    for i, tkr in enumerate(tickers, 1):
        res = process_ticker(tkr, start, end)
        if res is None:
            continue
        total_signals += res["signals"]
        total_hit50 += res["hit50"]
        total_hit100 += res["hit100"]
        all_rets.extend(res["max_rets"])
        if res["signals"] > 0:
            per_ticker_rows.append({
                "ticker": res["ticker"],
                "signals": res["signals"],
                "hit50": res["hit50"],
                "hit100": res["hit100"],
                "prec50": res["hit50"] / res["signals"] * 100,
                "prec100": res["hit100"] / res["signals"] * 100,
            })

        if i % 200 == 0:
            elapsed = time.time() - t0
            prec50 = (total_hit50 / total_signals * 100) if total_signals else 0
            prec100 = (total_hit100 / total_signals * 100) if total_signals else 0
            print(f"[{i}/{len(tickers)}] signals={total_signals} "
                  f"prec50={prec50:.2f}% prec100={prec100:.2f}% ({elapsed:.0f}s)")

    # ============================================================
    # 집계
    # ============================================================
    prec50 = (total_hit50 / total_signals * 100) if total_signals else 0.0
    prec100 = (total_hit100 / total_signals * 100) if total_signals else 0.0
    arr = np.array(all_rets, dtype=float) if all_rets else np.array([])

    lines = []
    lines.append("# 5-조건 필터 정밀도 검증 리포트")
    lines.append("")
    lines.append(f"- 생성일: {datetime.now():%Y-%m-%d %H:%M}")
    lines.append(f"- 샘플 종목 수: {len(tickers)}")
    lines.append(f"- 분석 기간: 최근 {LOOKBACK_YEARS}년")
    lines.append(f"- 보유 기간: {HOLD_DAYS} 거래일 (진입은 신호 다음날 시가)")
    lines.append(f"- 신호 쿨다운: {COOLDOWN} 거래일 (동일 종목)")
    lines.append("")

    lines.append("## 필터 조건")
    lines.append("1. 종가 > SMA20")
    lines.append("2. 30일 최대 거래량 >= 20일 평균의 5배")
    lines.append("3. 이벤트 직전 RSI(14) >= 50")
    lines.append("4. 30일 누적 수익률 >= 0%")
    lines.append("5. 종가 >= SMA50 * 1.10")
    lines.append("")

    lines.append("## 전체 결과")
    lines.append("")
    lines.append(f"- 총 신호 수: **{total_signals:,}건**")
    lines.append(f"- 5일 내 +50% 도달: **{total_hit50:,}건 ({prec50:.2f}%)**")
    lines.append(f"- 5일 내 +100% 도달: **{total_hit100:,}건 ({prec100:.2f}%)**")
    lines.append("")
    if len(arr) > 0:
        lines.append("### 5일 내 최대 수익률 분포 (고가 기준)")
        lines.append("")
        lines.append(f"- 평균: {arr.mean():.2f}%")
        lines.append(f"- 중앙값: {np.median(arr):.2f}%")
        lines.append(f"- P25 / P75: {np.percentile(arr,25):.2f}% / {np.percentile(arr,75):.2f}%")
        lines.append(f"- P90 / P95 / P99: {np.percentile(arr,90):.2f}% / {np.percentile(arr,95):.2f}% / {np.percentile(arr,99):.2f}%")
        lines.append(f"- 최대: {arr.max():.2f}%")
        lines.append("")

        lines.append("### 다양한 타겟별 도달률")
        lines.append("")
        lines.append("| 타겟 (5일 내 고가 기준) | 도달 건수 | 정밀도 |")
        lines.append("|---|---:|---:|")
        for t in [5, 10, 20, 30, 50, 75, 100, 150, 200]:
            hit = int((arr >= t).sum())
            pct = hit / len(arr) * 100
            lines.append(f"| +{t}% 이상 | {hit:,} | {pct:.2f}% |")
        lines.append("")

    # 종목 레벨 분포 (신호 발생한 종목만)
    if per_ticker_rows:
        pdf = pd.DataFrame(per_ticker_rows).sort_values("signals", ascending=False)
        lines.append("## 신호가 많이 발생한 종목 상위 30")
        lines.append("")
        lines.append("| 종목 | 신호수 | +50% 건 | +100% 건 | prec50% | prec100% |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for _, r in pdf.head(30).iterrows():
            lines.append(f"| {r['ticker']} | {int(r['signals'])} | {int(r['hit50'])} | "
                         f"{int(r['hit100'])} | {r['prec50']:.1f}% | {r['prec100']:.1f}% |")
        lines.append("")

        # CSV 저장
        try:
            pdf.to_csv(os.path.join(OUT_DIR, "precision_per_ticker.csv"), index=False)
        except Exception as e:
            print(f"[WARN] CSV save failed: {e}")

    lines.append("## 해석 가이드 (비전공자용)")
    lines.append("")
    lines.append("- **정밀도(Precision)** 는 '필터가 OK 라고 말한 것 중 진짜 급등한 비율' 이다.")
    lines.append("- 기준이 되는 **기저율(base rate)** 과 비교해야 의미가 있다.")
    lines.append("  예를 들어 랜덤하게 아무 날이나 골라서 5일 내 +50% 도달할 확률이 약 0.3%라면,")
    lines.append("  이 필터가 5%를 달성하면 '16배 개선' 이다.")
    lines.append("- 정밀도가 낮더라도 신호 수가 많으면 승률 40%대 전략도 기대값이 양수일 수 있다.")
    lines.append("- 신호가 특정 몇몇 종목에 쏠리면 과적합일 수 있으니, 상위 종목 분포도 함께 본다.")
    lines.append("")

    report = "\n".join(lines)
    out_md = os.path.join(OUT_DIR, "precision_report.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(report)

    elapsed = time.time() - t0
    print(f"[DONE] {elapsed/60:.1f} min. Signals={total_signals} "
          f"prec50={prec50:.2f}% prec100={prec100:.2f}%")
    print(f"Report: {out_md}")


if __name__ == "__main__":
    main()
