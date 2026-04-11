"""
원시 OHLCV 수집기 (Raw OHLCV Collector) — Stooq 버전
=====================================================
2020-01-01 ~ 오늘까지, 미국 주식 ~10,000 종목의 일봉 OHLCV를 수집해서
parquet 파일로 저장.

데이터 소스: Stooq (https://stooq.com)
  - 무료, API key 불필요
  - 직접 CSV 다운로드: https://stooq.com/q/d/l/?s={ticker}.us&i=d&d1=YYYYMMDD&d2=YYYYMMDD
  - 분할(split) 자동 조정 (배당 조정은 없음)
  - anti-bot/rate-limit 거의 없음 → GitHub Actions에서 정상 작동

원칙:
  1. 무료 데이터 소스만 사용
  2. 거짓 데이터 없음. 구할 수 없는 건 failed_tickers.csv 에 기록
  3. 단일 출처 (Stooq) → 데이터 일관성 보장
  4. 재시도 3회 후 실패 시 스킵
  5. 지표 계산 없음. 순수 OHLCV만 저장
  6. 한계: 현재 상장 종목만. 상장폐지 종목은 포함되지 않음 (생존자 편향 존재)

출력:
  data/raw/ohlcv/<TICKER>.parquet        (date, open, high, low, close, volume)
  data/raw/metadata/tickers.csv          (수집 성공 티커 목록)
  data/raw/metadata/failed_tickers.csv   (실패 티커 + 사유)
  data/raw/metadata/collection_log.json  (실행 로그)

실행:
  GitHub Actions workflow
  로컬: python research/data_collection/collect_raw_ohlcv.py
"""

import io
import json
import os
import random
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ============================================================
# 설정
# ============================================================
START_DATE = "2020-01-01"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "10000"))
MAX_RETRIES = 3

# Stooq는 rate limit이 거의 없어서 텀 짧게 유지
SLEEP_BETWEEN_TICKERS = 0.15
SLEEP_JITTER = 0.10
LONG_REST_EVERY_N = 1000           # 1000 티커마다 잠깐 휴식
LONG_REST_DURATION = 10
RETRY_BACKOFF = [2, 5, 10]

# 재개 모드
RESUME = os.environ.get("RESUME", "1") == "1"

# Stooq 엔드포인트
STOOQ_URL = "https://stooq.com/q/d/l/"
USER_AGENT = "Mozilla/5.0 (compatible; surge-scanner/1.0)"

# 파일 경로
ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw"
OHLCV_DIR = OUT_DIR / "ohlcv"
META_DIR = OUT_DIR / "metadata"
OHLCV_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

TICKERS_OUT = META_DIR / "tickers.csv"
FAILED_OUT = META_DIR / "failed_tickers.csv"
LOG_OUT = META_DIR / "collection_log.json"


# ============================================================
# 1. 티커 유니버스 수집
# ============================================================
def fetch_ticker_universe() -> pd.DataFrame:
    """
    무료 소스에서 현재 상장된 미국 주식 티커 목록을 수집.

    소스:
      - NASDAQ Trader FTP: nasdaqlisted.txt, otherlisted.txt (NASDAQ + NYSE + AMEX)
      - Wikipedia: S&P500 (보강)

    Returns:
      DataFrame with columns: ticker, exchange, source
    """
    rows = []

    # 1) NASDAQ listed
    try:
        df = pd.read_csv(
            "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
            sep="|",
        )
        df = df[df.get("Test Issue", "N") == "N"]
        for t in df["Symbol"].dropna().astype(str):
            rows.append({"ticker": t.strip(), "exchange": "NASDAQ", "source": "nasdaqtrader"})
        print(f"[INFO] NASDAQ listed: {len(df)} symbols")
    except Exception as e:
        print(f"[WARN] NASDAQ listed fetch failed: {e}")

    # 2) Other listed (NYSE, AMEX, etc.)
    try:
        df = pd.read_csv(
            "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
            sep="|",
        )
        df = df[df.get("Test Issue", "N") == "N"]
        exchange_map = {"N": "NYSE", "A": "AMEX", "P": "NYSE_ARCA", "Z": "BATS"}
        for _, r in df.iterrows():
            sym = str(r.get("ACT Symbol", "")).strip()
            ex = str(r.get("Exchange", "")).strip()
            if sym:
                rows.append({"ticker": sym, "exchange": exchange_map.get(ex, ex), "source": "nasdaqtrader"})
        print(f"[INFO] Other listed: {len(df)} symbols")
    except Exception as e:
        print(f"[WARN] Other listed fetch failed: {e}")

    # 3) Wikipedia S&P500 (보강)
    try:
        sp = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        for t in sp["Symbol"].astype(str):
            rows.append({"ticker": t.strip(), "exchange": "SP500", "source": "wikipedia"})
        print(f"[INFO] S&P500: {len(sp)} symbols")
    except Exception as e:
        print(f"[WARN] S&P500 fetch failed: {e}")

    if not rows:
        print("[FATAL] No tickers fetched from any source")
        sys.exit(1)

    df = pd.DataFrame(rows)

    # 클린업 (Stooq는 lowercase + 하이픈 표기)
    def clean(t):
        if not isinstance(t, str):
            return None
        t = t.strip().upper()
        if not t or t == "NAN":
            return None
        # BRK.B → BRK-B (Stooq 표준)
        t = t.replace(".", "-")
        if any(ch in t for ch in [" ", "$", "^", "/", "="]):
            return None
        if len(t) > 6:
            return None
        return t

    df["ticker"] = df["ticker"].apply(clean)
    df = df.dropna(subset=["ticker"])
    df = df.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    df = df.sort_values("ticker").reset_index(drop=True)
    print(f"[INFO] Total unique tickers: {len(df)}")
    return df


# ============================================================
# 2. Stooq에서 OHLCV 다운로드
# ============================================================
def stooq_url(ticker: str, start: str, end: str) -> str:
    """
    Stooq CSV 다운로드 URL 생성.
    예: https://stooq.com/q/d/l/?s=aapl.us&i=d&d1=20200101&d2=20260411
    """
    s = ticker.lower() + ".us"
    d1 = start.replace("-", "")
    d2 = end.replace("-", "")
    return f"{STOOQ_URL}?s={s}&i=d&d1={d1}&d2={d2}"


def download_single(ticker: str, start: str, end: str, session: requests.Session) -> pd.DataFrame | None:
    """
    Stooq에서 단일 티커 OHLCV CSV 다운로드.

    Returns:
      DataFrame [date, open, high, low, close, volume] 또는 None
    """
    url = stooq_url(ticker, start, end)
    try:
        resp = session.get(url, timeout=20, headers={"User-Agent": USER_AGENT})
    except Exception:
        return None

    if resp.status_code != 200:
        return None

    text = resp.text.strip()
    # Stooq는 데이터 없는 티커에 대해 "No data" 반환
    if not text or text.lower().startswith("no data") or "Date" not in text[:20]:
        return None

    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # 컬럼 표준화
    expected_cols = {"Date", "Open", "High", "Low", "Close", "Volume"}
    if not expected_cols.issubset(set(df.columns)):
        return None

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    out["open"] = pd.to_numeric(df["Open"], errors="coerce")
    out["high"] = pd.to_numeric(df["High"], errors="coerce")
    out["low"] = pd.to_numeric(df["Low"], errors="coerce")
    out["close"] = pd.to_numeric(df["Close"], errors="coerce")
    out["volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # 핵심 가격이 NaN인 행 제외
    out = out.dropna(subset=["open", "high", "low", "close"], how="all")
    out = out.reset_index(drop=True)

    if out.empty:
        return None
    return out


def save_ticker(ticker: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    path = OHLCV_DIR / f"{ticker}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    return len(df)


# ============================================================
# 3. 메인 루프
# ============================================================
def main():
    print("=" * 60)
    print("원시 OHLCV 수집기 (Stooq)")
    print(f"기간: {START_DATE} ~ {END_DATE}")
    print(f"최대 티커: {MAX_TICKERS}")
    print("=" * 60)

    start_time = time.time()

    # 1) 티커 유니버스
    universe = fetch_ticker_universe()
    if len(universe) > MAX_TICKERS:
        universe = universe.head(MAX_TICKERS)
    tickers = universe["ticker"].tolist()
    print(f"[INFO] 수집 대상: {len(tickers)} 종목")

    # HTTP 세션 재사용
    session = requests.Session()

    # 2) 개별 다운로드
    success_rows = []
    failed_rows = []

    total = len(tickers)
    skipped_resume = 0
    consecutive_fail = 0

    for idx, t in enumerate(tickers, start=1):
        # 재개 모드
        if RESUME and (OHLCV_DIR / f"{t}.parquet").exists():
            skipped_resume += 1
            continue

        # 재시도
        df = None
        for attempt in range(1, MAX_RETRIES + 1):
            df = download_single(t, START_DATE, END_DATE, session)
            if df is not None and not df.empty:
                break
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
                time.sleep(wait)

        if df is None or df.empty:
            failed_rows.append({"ticker": t, "reason": "no_data_from_stooq"})
            consecutive_fail += 1
            # 연속 30개 실패 시 30초 쿨다운 (Stooq가 막을 가능성 낮지만 안전장치)
            if consecutive_fail >= 30:
                print(f"  [경고] 연속 {consecutive_fail}개 실패. 30초 대기...")
                time.sleep(30)
                consecutive_fail = 0
        else:
            consecutive_fail = 0
            try:
                n = save_ticker(t, df)
                meta = universe[universe["ticker"] == t].iloc[0]
                success_rows.append({
                    "ticker": t,
                    "exchange": meta["exchange"],
                    "source": meta["source"],
                    "n_rows": n,
                    "first_date": df["date"].iloc[0],
                    "last_date": df["date"].iloc[-1],
                })
            except Exception as e:
                failed_rows.append({"ticker": t, "reason": f"save_error: {type(e).__name__}: {e}"})

        # 진행 로그 (100 티커마다)
        if idx % 100 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            eta_sec = (total - idx) / rate if rate > 0 else 0
            print(f"[{idx}/{total}] success={len(success_rows)} failed={len(failed_rows)} "
                  f"skipped={skipped_resume} elapsed={elapsed:.0f}s eta={eta_sec/60:.1f}min")

        # 중간 저장 (500 티커마다)
        if idx % 500 == 0:
            pd.DataFrame(success_rows).to_csv(TICKERS_OUT, index=False)
            pd.DataFrame(failed_rows).to_csv(FAILED_OUT, index=False)

        # 긴 휴식 (1000 티커마다)
        if idx % LONG_REST_EVERY_N == 0:
            print(f"  [잠깐 휴식] {LONG_REST_DURATION}초 대기...")
            time.sleep(LONG_REST_DURATION)
        else:
            # 티커 간 짧은 텀
            time.sleep(SLEEP_BETWEEN_TICKERS + random.uniform(0, SLEEP_JITTER))

    # 3) 최종 저장
    pd.DataFrame(success_rows).to_csv(TICKERS_OUT, index=False)
    pd.DataFrame(failed_rows).to_csv(FAILED_OUT, index=False)

    elapsed = time.time() - start_time
    log = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_date": START_DATE,
        "end_date": END_DATE,
        "requested_tickers": len(tickers),
        "succeeded": len(success_rows),
        "failed": len(failed_rows),
        "skipped_resume": skipped_resume,
        "elapsed_seconds": round(elapsed, 1),
        "data_source": "stooq.com (free CSV)",
        "ticker_sources": ["nasdaqtrader.com", "wikipedia S&P500"],
        "split_adjusted": True,
        "dividend_adjusted": False,
        "limitations": [
            "현재 상장 종목만 포함 (상장폐지 종목 제외, 생존자 편향 존재)",
            "Stooq에 등록되지 않은 일부 마이크로캡 누락 가능",
            "배당 조정 없음 (분할만 조정됨)",
        ],
        "output_dir": str(OHLCV_DIR.relative_to(ROOT)),
    }
    with open(LOG_OUT, "w") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

    print("=" * 60)
    print(f"완료. 성공: {len(success_rows)}  실패: {len(failed_rows)}  스킵: {skipped_resume}")
    print(f"경과: {elapsed/60:.1f}분")
    print(f"출력: {OHLCV_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
