"""
Surge Window Analysis
---------------------
급등 발생 이전 30일 / 60일 윈도우의 특징을 추출하고,
각 특징(feature)에 대해 여러 임계값(threshold)별로
'발생건수 대비 몇 %가 이 조건을 만족했는가'를 계산한다.

- 5개 독립 구간 (bin):
    day_50_100      : 1일 수익률 50~100%
    day_100_200     : 1일 수익률 100~200%
    day_200plus     : 1일 수익률 200% 이상
    week_50_100     : 5일(주간) 수익률 50~100%
    week_100plus    : 5일(주간) 수익률 100% 이상

- 윈도우 길이: 30일, 60일 (둘 다)
- 출력: data/surge_window_report.md (텍스트 전용)
- 데이터: yfinance 무료 데이터만 사용
- 기본 샘플: 10,000 종목 (환경변수 SAMPLE_SIZE로 조정)

작성 목표: 비전공자도 읽을 수 있도록 한글 설명 포함.
"""

import os
import sys
import time
import math
import json
import random
import warnings
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ============================================================
# 0. 설정
# ============================================================

SAMPLE_SIZE = int(os.environ.get("SAMPLE_SIZE", "10000"))
LOOKBACK_YEARS = 5
MIN_PRICE = 2.0          # 페니주 제거
MIN_DOLLAR_VOL = 500_000 # 거래대금 50만 달러 미만 제거

WINDOWS = [30, 60]  # 윈도우 길이 (영업일 기준)

# 독립 구간 정의
BINS = [
    "day_50_100",
    "day_100_200",
    "day_200plus",
    "week_50_100",
    "week_100plus",
]

BIN_LABELS = {
    "day_50_100":  "1일 +50~100%",
    "day_100_200": "1일 +100~200%",
    "day_200plus": "1일 +200% 이상",
    "week_50_100": "5일 +50~100%",
    "week_100plus":"5일 +100% 이상",
}

OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)


# ============================================================
# 1. 종목 리스트 수집 (무료, yfinance)
# ============================================================

def get_ticker_universe(n=10000):
    """Wikipedia / stooq 등을 통한 무료 티커 리스트.
    yfinance 자체가 공식 티커 리스트를 제공하지 않아,
    SP500 + NASDAQ Composite + NYSE 상장 리스트를 조합한다.
    실패 시 최소한 S&P500 + 추가 목록으로 fallback.
    """
    tickers = set()

    # 1) S&P500
    try:
        sp = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        tickers.update(sp["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist())
    except Exception as e:
        print(f"[WARN] SP500 fetch failed: {e}")

    # 2) NASDAQ listed (NASDAQ Trader)
    try:
        url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        df = pd.read_csv(url, sep="|")
        df = df[df["Test Issue"] == "N"]
        tickers.update(df["Symbol"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] NASDAQ listed fetch failed: {e}")

    # 3) Other listed (NYSE, AMEX 등)
    try:
        url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
        df = pd.read_csv(url, sep="|")
        df = df[df["Test Issue"] == "N"]
        tickers.update(df["ACT Symbol"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Other listed fetch failed: {e}")

    # clean
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
# 2. 지표 계산
# ============================================================

def rsi_wilder(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_dn = dn.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_dn.replace(0, np.nan))
    return 100 - (100 / (1 + rs))


def atr(df, period=14):
    h, l, c = df["High"], df["Low"], df["Close"]
    tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def add_indicators(df):
    c = df["Close"]
    v = df["Volume"]
    df["ret1"] = c.pct_change()
    df["ret5"] = c.pct_change(5)
    df["sma20"] = c.rolling(20).mean()
    df["sma50"] = c.rolling(50).mean()
    df["dist_sma20"] = (c / df["sma20"] - 1)
    df["dist_sma50"] = (c / df["sma50"] - 1)
    df["vol20"] = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol20"]
    df["rsi14"] = rsi_wilder(c, 14)
    df["atr14"] = atr(df, 14)
    df["atr_pct"] = df["atr14"] / c
    df["std20"] = df["ret1"].rolling(20).std()
    df["high60"] = c.rolling(60).max()
    df["low60"] = c.rolling(60).min()
    df["dist_high60"] = (c / df["high60"] - 1)
    df["dist_low60"] = (c / df["low60"] - 1)
    # Bollinger
    m = c.rolling(20).mean()
    s = c.rolling(20).std()
    df["bb_width"] = (4 * s) / m  # (upper-lower)/mid
    # Dollar volume
    df["dollar_vol"] = c * v
    return df


# ============================================================
# 3. 이벤트 탐색 + bin 분류
# ============================================================

def classify_bin(ret1, ret5):
    if ret1 >= 2.0:
        return "day_200plus"
    if ret1 >= 1.0:
        return "day_100_200"
    if ret1 >= 0.5:
        return "day_50_100"
    if ret5 >= 1.0:
        return "week_100plus"
    if ret5 >= 0.5:
        return "week_50_100"
    return None


# ============================================================
# 4. 윈도우 특징 추출
# ============================================================

def extract_window_features(win):
    """윈도우 기간의 OHLCV + 지표를 받아 특징을 추출."""
    f = {}
    c = win["Close"].values
    v = win["Volume"].values
    n = len(c)
    if n < 10:
        return None

    # --- 가격 추세 ---
    x = np.arange(n)
    # 정규화된 종가에 대한 선형회귀 기울기 (단위: %/일)
    norm = c / c[0]
    slope = np.polyfit(x, norm, 1)[0] * 100  # % per day
    f["trend_slope_pct_per_day"] = slope
    f["total_return_in_window"] = (c[-1] / c[0] - 1) * 100

    # 윈도우 고점 대비 현재가 거리
    wmax = c.max()
    wmin = c.min()
    f["dist_from_win_high"] = (c[-1] / wmax - 1) * 100  # 음수
    f["dist_from_win_low"] = (c[-1] / wmin - 1) * 100   # 양수
    f["win_range_pct"] = (wmax / wmin - 1) * 100

    # --- 변동성 ---
    rets = pd.Series(c).pct_change().dropna().values
    if len(rets) > 0:
        f["avg_daily_vol_pct"] = np.std(rets) * 100
        # 앞/뒤 절반 변동성 비율 (수축/팽창)
        half = len(rets) // 2
        if half >= 3:
            first = np.std(rets[:half]) + 1e-9
            last = np.std(rets[half:]) + 1e-9
            f["vol_contraction_ratio"] = last / first  # <1 = 수축, >1 = 팽창
        else:
            f["vol_contraction_ratio"] = np.nan
    else:
        f["avg_daily_vol_pct"] = np.nan
        f["vol_contraction_ratio"] = np.nan

    # --- 거래량 ---
    if "vol20" in win and not win["vol20"].isna().all():
        avg20 = win["vol20"].mean()
        vmax = v.max()
        f["max_volume_ratio"] = vmax / avg20 if avg20 > 0 else np.nan
        # 윈도우 뒷 10일 거래량 / 앞 10일 거래량
        if n >= 20:
            f["volume_ramp_ratio"] = (v[-10:].mean() / (v[:10].mean() + 1e-9))
        else:
            f["volume_ramp_ratio"] = np.nan
    else:
        f["max_volume_ratio"] = np.nan
        f["volume_ramp_ratio"] = np.nan

    # --- RSI ---
    if "rsi14" in win:
        rsi = win["rsi14"].dropna().values
        if len(rsi) > 0:
            f["max_rsi"] = float(np.max(rsi))
            f["min_rsi"] = float(np.min(rsi))
            f["last_rsi"] = float(rsi[-1])
            f["rsi_range"] = float(np.max(rsi) - np.min(rsi))
        else:
            f["max_rsi"] = f["min_rsi"] = f["last_rsi"] = f["rsi_range"] = np.nan

    # --- Bollinger 수축 ---
    if "bb_width" in win:
        bbw = win["bb_width"].dropna().values
        if len(bbw) > 0:
            f["min_bb_width"] = float(np.min(bbw))
            f["last_bb_width"] = float(bbw[-1])
            f["bb_squeeze_ratio"] = float(bbw[-1] / (np.max(bbw) + 1e-9))
        else:
            f["min_bb_width"] = f["last_bb_width"] = f["bb_squeeze_ratio"] = np.nan

    # --- 컨솔리데이션 (횡보일수) ---
    # |일일수익률| < 2% 인 날의 비율
    if len(rets) > 0:
        f["sideways_day_pct"] = (np.abs(rets) < 0.02).mean() * 100
        f["big_up_day_pct"] = (rets > 0.05).mean() * 100
        f["big_down_day_pct"] = (rets < -0.05).mean() * 100

    # --- ATR ---
    if "atr_pct" in win:
        atrp = win["atr_pct"].dropna().values
        if len(atrp) > 0:
            f["avg_atr_pct"] = float(np.mean(atrp)) * 100
            f["last_atr_pct"] = float(atrp[-1]) * 100

    # --- 52일 신고가/신저가 근접 ---
    if "dist_high60" in win and "dist_low60" in win:
        f["last_dist_high60_pct"] = float(win["dist_high60"].iloc[-1] * 100)
        f["last_dist_low60_pct"] = float(win["dist_low60"].iloc[-1] * 100)

    # --- 이동평균 위/아래 ---
    if "dist_sma20" in win and "dist_sma50" in win:
        d20 = win["dist_sma20"].dropna()
        d50 = win["dist_sma50"].dropna()
        if len(d20) > 0:
            f["days_above_sma20_pct"] = (d20 > 0).mean() * 100
            f["last_dist_sma20_pct"] = float(d20.iloc[-1] * 100)
        if len(d50) > 0:
            f["days_above_sma50_pct"] = (d50 > 0).mean() * 100
            f["last_dist_sma50_pct"] = float(d50.iloc[-1] * 100)

    # --- 가격대 ---
    f["price_at_event_entry"] = float(c[-1])

    return f


# ============================================================
# 5. 종목 스캔
# ============================================================

def scan_ticker(ticker, start, end):
    try:
        df = yf.download(ticker, start=start, end=end,
                         auto_adjust=True, progress=False, threads=False)
        if df is None or df.empty or len(df) < 120:
            return []
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = add_indicators(df)
    except Exception:
        return []

    events = []
    # 최소 60일 이후부터 이벤트 가능 (60일 윈도우)
    for i in range(60, len(df) - 1):
        row = df.iloc[i]
        close = row["Close"]
        dvol = row["dollar_vol"]
        if pd.isna(close) or pd.isna(dvol):
            continue
        if close < MIN_PRICE or dvol < MIN_DOLLAR_VOL:
            continue

        r1 = row["ret1"]
        r5 = row["ret5"]
        if pd.isna(r1) or pd.isna(r5):
            continue

        bin_name = classify_bin(r1, r5)
        if bin_name is None:
            continue

        # 윈도우 추출 (이벤트일 제외, 이벤트 직전까지)
        for w in WINDOWS:
            if i - w < 0:
                continue
            window_df = df.iloc[i - w:i]  # 이벤트일 제외
            feats = extract_window_features(window_df)
            if feats is None:
                continue
            feats["ticker"] = ticker
            feats["date"] = str(df.index[i].date())
            feats["bin"] = bin_name
            feats["window"] = w
            feats["ret1"] = float(r1)
            feats["ret5"] = float(r5)
            events.append(feats)

    return events


# ============================================================
# 6. 임계값 정의 (각 feature마다 여러 단계)
# ============================================================

# 각 feature에 대해 여러 임계값, 그리고 부등호 방향.
# direction: ">=" 또는 "<="
FEATURE_THRESHOLDS = {
    "trend_slope_pct_per_day": (">=", [0.0, 0.1, 0.2, 0.5, 1.0], "추세 기울기(%/일)"),
    "total_return_in_window":  (">=", [0, 10, 20, 50, 100], "윈도우 누적 수익률(%)"),
    "dist_from_win_high":      (">=", [-30, -20, -10, -5, -2], "윈도우 고점 대비 거리(%)"),
    "dist_from_win_low":       (">=", [5, 20, 50, 100, 200], "윈도우 저점 대비 거리(%)"),
    "win_range_pct":           (">=", [20, 50, 100, 200, 500], "윈도우 고저 변동폭(%)"),
    "avg_daily_vol_pct":       (">=", [2, 4, 6, 8, 12], "일 평균 변동성(%)"),
    "vol_contraction_ratio":   ("<=", [1.0, 0.8, 0.6, 0.5, 0.3], "변동성 수축 비율(후반/전반)"),
    "max_volume_ratio":        (">=", [2, 3, 5, 10, 20], "최대 거래량/20일 평균"),
    "volume_ramp_ratio":       (">=", [1.0, 1.5, 2.0, 3.0, 5.0], "후반 10일/전반 10일 거래량비"),
    "max_rsi":                 (">=", [50, 60, 70, 80, 90], "윈도우 최고 RSI"),
    "min_rsi":                 ("<=", [50, 40, 30, 25, 20], "윈도우 최저 RSI"),
    "last_rsi":                (">=", [40, 50, 60, 70, 80], "이벤트 직전 RSI"),
    "bb_squeeze_ratio":        ("<=", [1.0, 0.7, 0.5, 0.3, 0.2], "BB 최근폭/최대폭"),
    "sideways_day_pct":        (">=", [30, 50, 60, 70, 80], "횡보일(|일수익|<2%) 비율(%)"),
    "big_up_day_pct":          (">=", [0, 5, 10, 20, 30], "+5% 이상 상승일 비율(%)"),
    "big_down_day_pct":        (">=", [0, 5, 10, 20, 30], "-5% 이상 하락일 비율(%)"),
    "avg_atr_pct":             (">=", [2, 4, 6, 8, 12], "평균 ATR%"),
    "last_dist_high60_pct":    (">=", [-30, -15, -5, -1, 0], "60일 고점 대비 거리(%)"),
    "last_dist_low60_pct":     (">=", [10, 30, 50, 100, 200], "60일 저점 대비 거리(%)"),
    "days_above_sma20_pct":    (">=", [30, 50, 70, 85, 95], "SMA20 위 일수 비율(%)"),
    "days_above_sma50_pct":    (">=", [30, 50, 70, 85, 95], "SMA50 위 일수 비율(%)"),
    "last_dist_sma20_pct":     (">=", [-20, -5, 0, 10, 30], "SMA20 대비 이격(%)"),
    "last_dist_sma50_pct":     (">=", [-20, -5, 0, 10, 30], "SMA50 대비 이격(%)"),
    "price_at_event_entry":    (">=", [2, 5, 10, 20, 50], "이벤트 진입가($)"),
}


def compute_commonality(events, feat, direction, thresholds):
    """events: list of dict. 특정 feature에 대해 각 임계값 별 비율 계산."""
    vals = [e.get(feat) for e in events if e.get(feat) is not None and not (isinstance(e.get(feat), float) and math.isnan(e.get(feat)))]
    n = len(vals)
    if n == 0:
        return None, 0
    arr = np.array(vals, dtype=float)
    result = []
    for t in thresholds:
        if direction == ">=":
            pct = (arr >= t).mean() * 100
        else:
            pct = (arr <= t).mean() * 100
        result.append((t, pct))
    return result, n


def stats(events, feat):
    vals = [e.get(feat) for e in events if e.get(feat) is not None and not (isinstance(e.get(feat), float) and math.isnan(e.get(feat)))]
    if not vals:
        return None
    a = np.array(vals, dtype=float)
    return {
        "n": len(a),
        "median": float(np.median(a)),
        "mean": float(np.mean(a)),
        "p25": float(np.percentile(a, 25)),
        "p75": float(np.percentile(a, 75)),
    }


# ============================================================
# 7. 메인
# ============================================================

def main():
    t0 = time.time()
    print(f"[INFO] Sample size: {SAMPLE_SIZE}")
    tickers = get_ticker_universe(SAMPLE_SIZE)
    print(f"[INFO] Universe collected: {len(tickers)} tickers")

    end = datetime.today()
    start = end - timedelta(days=365 * LOOKBACK_YEARS + 30)

    all_events = []
    done = 0
    fail = 0
    for t in tickers:
        done += 1
        try:
            evs = scan_ticker(t, start, end)
            if evs:
                all_events.extend(evs)
        except Exception:
            fail += 1
        if done % 200 == 0:
            elapsed = time.time() - t0
            print(f"[INFO] {done}/{len(tickers)} processed, events={len(all_events)}, fail={fail}, {elapsed:.0f}s")

    print(f"[INFO] Total events collected: {len(all_events)}")

    # bin × window 별 분리
    grouped = defaultdict(list)
    for e in all_events:
        grouped[(e["bin"], e["window"])].append(e)

    # 카운트 출력
    counts = {}
    for b in BINS:
        for w in WINDOWS:
            counts[(b, w)] = len(grouped[(b, w)])
    print("[INFO] Counts:", counts)

    # ============================================================
    # 리포트 작성
    # ============================================================
    lines = []
    lines.append("# 급등 전 공통 지표 탐색 리포트 (Surge Window Analysis)")
    lines.append("")
    lines.append(f"- 생성일: {datetime.now():%Y-%m-%d %H:%M}")
    lines.append(f"- 샘플 종목 수: {len(tickers)}")
    lines.append(f"- 분석 기간: 최근 {LOOKBACK_YEARS}년")
    lines.append(f"- 총 이벤트 수(중복 window 포함): {len(all_events)}")
    lines.append("")
    lines.append("## 분석 방법 요약")
    lines.append("1. 5개 독립 구간(bin)에 해당하는 급등 이벤트를 수집했다.")
    lines.append("2. 각 이벤트 직전 30일 / 60일 윈도우를 추출했다.")
    lines.append("3. 윈도우에서 추세, 변동성, 거래량, RSI, 이동평균 등 특징을 계산했다.")
    lines.append("4. 각 특징에 대해 여러 임계값을 설정하고, 이벤트의 몇 %가 해당 임계값을 만족했는지 비율을 구했다.")
    lines.append("5. 비율이 높을수록 '급등 직전에 자주 관찰되는 공통 지표'라고 판단할 수 있다.")
    lines.append("")
    lines.append("## 이벤트 건수")
    lines.append("")
    lines.append("| 구간 | 30일 윈도우 | 60일 윈도우 |")
    lines.append("|---|---:|---:|")
    for b in BINS:
        lines.append(f"| {BIN_LABELS[b]} | {counts[(b,30)]:,} | {counts[(b,60)]:,} |")
    lines.append("")

    # feature table per (bin, window)
    for b in BINS:
        lines.append(f"## 구간: {BIN_LABELS[b]}")
        lines.append("")
        for w in WINDOWS:
            evs = grouped[(b, w)]
            n_total = len(evs)
            lines.append(f"### 윈도우 {w}일 (이벤트 {n_total:,}건)")
            lines.append("")
            if n_total == 0:
                lines.append("_데이터 없음_")
                lines.append("")
                continue

            for feat, (direction, thresholds, label) in FEATURE_THRESHOLDS.items():
                res, n = compute_commonality(evs, feat, direction, thresholds)
                st = stats(evs, feat)
                if res is None or st is None:
                    continue
                lines.append(f"**{label}** (`{feat}`, 유효 {n:,}건)")
                lines.append(f"- 중앙값 {st['median']:.3f} / 평균 {st['mean']:.3f} / P25 {st['p25']:.3f} / P75 {st['p75']:.3f}")
                header = "| 임계값 | 조건 | 만족 비율 |"
                sep    = "|---|---|---:|"
                lines.append(header)
                lines.append(sep)
                for t, pct in res:
                    cond = f"{direction} {t}"
                    lines.append(f"| {t} | {cond} | {pct:.1f}% |")
                lines.append("")
            lines.append("")

    # ============================================================
    # 구간간 비교: 각 feature, 각 threshold에서 bin별 % 비교표
    # (어떤 지표가 특정 bin에 유난히 치우쳐 있는지 파악)
    # ============================================================
    lines.append("## 구간별 비교 요약 (30일 윈도우 기준)")
    lines.append("")
    lines.append("같은 임계값에서 bin별 비율 차이가 크면, 그 지표는 해당 bin의 특징이다.")
    lines.append("")
    for feat, (direction, thresholds, label) in FEATURE_THRESHOLDS.items():
        lines.append(f"### {label} (`{feat}`)")
        header = "| 임계값 | " + " | ".join([BIN_LABELS[b] for b in BINS]) + " |"
        sep = "|---|" + "---:|" * len(BINS)
        lines.append(header)
        lines.append(sep)
        for t in thresholds:
            row = [f"{direction} {t}"]
            for b in BINS:
                evs = grouped[(b, 30)]
                if not evs:
                    row.append("-")
                    continue
                vals = [e.get(feat) for e in evs if e.get(feat) is not None and not (isinstance(e.get(feat), float) and math.isnan(e.get(feat)))]
                if not vals:
                    row.append("-")
                    continue
                arr = np.array(vals, dtype=float)
                if direction == ">=":
                    pct = (arr >= t).mean() * 100
                else:
                    pct = (arr <= t).mean() * 100
                row.append(f"{pct:.1f}%")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

    # 60일 비교도 추가
    lines.append("## 구간별 비교 요약 (60일 윈도우 기준)")
    lines.append("")
    for feat, (direction, thresholds, label) in FEATURE_THRESHOLDS.items():
        lines.append(f"### {label} (`{feat}`)")
        header = "| 임계값 | " + " | ".join([BIN_LABELS[b] for b in BINS]) + " |"
        sep = "|---|" + "---:|" * len(BINS)
        lines.append(header)
        lines.append(sep)
        for t in thresholds:
            row = [f"{direction} {t}"]
            for b in BINS:
                evs = grouped[(b, 60)]
                if not evs:
                    row.append("-")
                    continue
                vals = [e.get(feat) for e in evs if e.get(feat) is not None and not (isinstance(e.get(feat), float) and math.isnan(e.get(feat)))]
                if not vals:
                    row.append("-")
                    continue
                arr = np.array(vals, dtype=float)
                if direction == ">=":
                    pct = (arr >= t).mean() * 100
                else:
                    pct = (arr <= t).mean() * 100
                row.append(f"{pct:.1f}%")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

    report = "\n".join(lines)
    out_md = os.path.join(OUT_DIR, "surge_window_report.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(report)

    # 원본 이벤트도 CSV로 저장
    try:
        pd.DataFrame(all_events).to_csv(
            os.path.join(OUT_DIR, "surge_window_events.csv"),
            index=False
        )
    except Exception as e:
        print(f"[WARN] CSV save failed: {e}")

    elapsed = time.time() - t0
    print(f"[DONE] {elapsed/60:.1f} min. Report: {out_md}")


if __name__ == "__main__":
    main()
