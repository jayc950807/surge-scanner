"""
원시 OHLCV 수집기 (Raw OHLCV Collector)
=====================================
2020-01-01 ~ 오늘까지, 미국 주식 ~10,000 종목의 일봉 OHLCV를 수집해서
parquet 파일로 저장.

원칙:
  1. 무료 데이터 소스만 사용 (yfinance + NASDAQ Trader FTP + Wikipedia)
  2. 거짓 데이터 없음. 구할 수 없는 건 failed_tickers.csv 에 기록
  3. 날짜 정합성 보장 (UTC 기준 거래일)
  4. 재시도 3회 후 실패 시 스킵
  5. 지표 계산 없음. 순수 OHLCV + adjusted close + volume만 저장

출력:
  data/raw/ohlcv/<TICKER>.parquet        (컬럼: date, open, high, low, close, adj_close, volume)
  data/raw/metadata/tickers.csv          (수집 성공 티커 목록 + 거래소)
  data/raw/metadata/failed_tickers.csv   (실패 티커 + 사유)
  data/raw/metadata/collection_log.json  (실행 로그)

실행:
  GitHub Actions workflow (data/raw/ 디렉토리로 커밋됨)
  로컬: python research/data_collection/collect_raw_ohlcv.py
"""

import json
import os
import random
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ============================================================
# 설정
# ============================================================
START_DATE = "2020-01-01"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "10000"))
MAX_RETRIES = 3

# Rate limit 대응 (티커 1개씩 순차 다운로드)
# Yahoo Finance가 yf.download() 배치 API를 차단하므로
# 기존 precision_check_fg.py와 동일하게 개별 다운로드 방식 사용
SLEEP_BETWEEN_TICKERS = 0.4        # 초 (티커 간 텀)
SLEEP_JITTER = 0.3                 # ± 랜덤 (0~0.3초)
LONG_REST_EVERY_N = 500            # 500 티커마다 긴 휴식
LONG_REST_DURATION = 20            # 긴 휴식 20초
RETRY_BACKOFF = [3, 10, 20]        # 재시도 대기(초)

# 재개 가능 (이미 저장된 티커 스킵)
RESUME = os.environ.get("RESUME", "1") == "1"

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
      - Wikipedia: S&P500, Dow30, NASDAQ100 (보강)

    Returns:
      DataFrame with columns: ticker, exchange, source
    """
    rows = []

    # 1) NASDAQ listed (NASDAQ Global/Select)
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

    # 2) Other listed (NYSE, AMEX, BATS, etc.)
    try:
        df = pd.read_csv(
            "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
            sep="|",
        )
        df = df[df.get("Test Issue", "N") == "N"]
        for _, r in df.iterrows():
            sym = str(r.get("ACT Symbol", "")).strip()
            ex = str(r.get("Exchange", "")).strip()
            exchange_map = {"N": "NYSE", "A": "AMEX", "P": "NYSE_ARCA", "Z": "BATS"}
            if sym:
                rows.append({"ticker": sym, "exchange": exchange_map.get(ex, ex), "source": "nasdaqtrader"})
        print(f"[INFO] Other listed: {len(df)} symbols")
    except Exception as e:
        print(f"[WARN] Other listed fetch failed: {e}")

    # 3) Wikipedia S&P500 (보강, 중복은 자동 dedupe)
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

    # 클린업
    def clean(t):
        if not isinstance(t, str):
            return None
        t = t.strip().upper()
        if not t or t == "NAN":
            return None
        # yfinance는 BRK.B 를 BRK-B 로 표기
        t = t.replace(".", "-")
        # 특수문자/공백 제외
        if any(ch in t for ch in [" ", "$", "^", "/", "="]):
            return None
        if len(t) > 6:  # 일반 종목 최대 5자
            return None
        return t

    df["ticker"] = df["ticker"].apply(clean)
    df = df.dropna(subset=["ticker"])
    # 동일 티커가 여러 소스에 있으면 첫 번째 유지
    df = df.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    df = df.sort_values("ticker").reset_index(drop=True)
    print(f"[INFO] Total unique tickers: {len(df)}")
    return df


# ============================================================
# 2. OHLCV 다운로드 (개별 티커 단위)
# ============================================================
def download_single(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """
    단일 티커 다운로드. 기존 precision_check_fg.py 와 동일한 방식.

    Returns:
      DataFrame 또는 None (실패 시)
    """
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as e:
        return None

    if df is None or df.empty:
        return None

    # MultiIndex columns 해제 (가끔 단일 티커도 MultiIndex로 나옴)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.dropna(how="all")
    if df.empty:
        return None
    return df


def normalize_ohlcv(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    표준 스키마로 정규화.
      date | open | high | low | close | adj_close | volume
    """
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df.index).tz_localize(None).strftime("%Y-%m-%d")
    out["open"] = df.get("Open")
    out["high"] = df.get("High")
    out["low"] = df.get("Low")
    out["close"] = df.get("Close")
    out["adj_close"] = df.get("Adj Close")
    out["volume"] = df.get("Volume")

    # 전부 NaN인 행 제외
    out = out.dropna(subset=["open", "high", "low", "close"], how="all")
    out = out.reset_index(drop=True)
    return out


def save_ticker(ticker: str, df: pd.DataFrame) -> int:
    """
    parquet 저장. 저장된 행 수 반환.
    """
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
    print("원시 OHLCV 수집기 (티커 단위 순차 다운로드)")
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

    # 2) 개별 다운로드
    success_rows = []    # [{ticker, exchange, source, n_rows, first_date, last_date}]
    failed_rows = []     # [{ticker, reason}]

    total = len(tickers)
    skipped_resume = 0
    consecutive_empty = 0  # 연속 실패 카운터 (rate limit 감지)

    for idx, t in enumerate(tickers, start=1):
        # 재개 모드: 이미 저장된 티커는 스킵
        if RESUME and (OHLCV_DIR / f"{t}.parquet").exists():
            skipped_resume += 1
            continue

        # 재시도 루프
        df = None
        for attempt in range(1, MAX_RETRIES + 1):
            df = download_single(t, START_DATE, END_DATE)
            if df is not None and not df.empty:
                break
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
                time.sleep(wait)

        if df is None or df.empty:
            failed_rows.append({"ticker": t, "reason": "no_data_from_yfinance"})
            consecutive_empty += 1
            # 연속 20개 실패 = rate limit 의심 → 60초 쿨다운
            if consecutive_empty >= 20:
                print(f"  [rate limit 의심] 연속 {consecutive_empty}개 실패. 60초 대기...")
                time.sleep(60)
                consecutive_empty = 0
        else:
            consecutive_empty = 0
            try:
                norm = normalize_ohlcv(df, t)
                if norm.empty:
                    failed_rows.append({"ticker": t, "reason": "empty_after_normalize"})
                else:
                    n = save_ticker(t, norm)
                    meta = universe[universe["ticker"] == t].iloc[0]
                    success_rows.append({
                        "ticker": t,
                        "exchange": meta["exchange"],
                        "source": meta["source"],
                        "n_rows": n,
                        "first_date": norm["date"].iloc[0],
                        "last_date": norm["date"].iloc[-1],
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

        # 긴 휴식 (500 티커마다)
        if idx % LONG_REST_EVERY_N == 0:
            print(f"  [long rest] {LONG_REST_DURATION}초 대기...")
            time.sleep(LONG_REST_DURATION)
        else:
            # 티커 간 텀 + 지터
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
        "data_source": "yfinance (single-ticker sequential)",
        "ticker_sources": ["nasdaqtrader.com", "wikipedia S&P500"],
        "output_dir": str(OHLCV_DIR.relative_to(ROOT)),
    }
    with open(LOG_OUT, "w") as f:
        json.dump(log, f, indent=2)

    print("=" * 60)
    print(f"완료. 성공: {len(success_rows)}  실패: {len(failed_rows)}")
    print(f"경과: {elapsed/60:.1f}분")
    print(f"출력: {OHLCV_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
