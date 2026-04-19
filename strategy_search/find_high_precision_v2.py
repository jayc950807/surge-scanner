#!/usr/bin/env python3
"""
find_high_precision_v2.py — Tracker-Compatible Backtest
Uses tracker.py's exact track_position_daywise() rules:
  - Same-day TP+SL = LOSS (conservative)
  - EXPIRED after max_hold = failure
  - Entry at signal_date close, D+1 tracking start
  - SL = -20% always applied

Usage:
  python find_high_precision_v2.py --thresh 0.10 --period 5
  python find_high_precision_v2.py --thresh 0.10 --period 5 --combo 3
  python find_high_precision_v2.py --all  (runs all 24 combinations)
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import sys
import time
import warnings
from datetime import datetime, timezone
from itertools import combinations

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Import shared helpers
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shared_config import (
    LEVERAGED_ETF,
    calc_rsi_wilder,
    get_all_tickers,
    download_batch,
    extract_ticker_df,
    BATCH_SIZE,
    BATCH_DELAY,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALL_THRESHOLDS = [0.10, 0.15, 0.20, 0.30]
ALL_PERIODS = [3, 5, 7, 10, 15, 20]
SL_PCT = -0.20  # always -20%

CONDITION_NAMES = [
    "high_vol", "very_high_vol",
    "rsi_below_30", "rsi_below_40", "rsi_30_50",
    "near_52w_low", "deep_52w_low",
    "vol_1_5x", "vol_2x", "vol_3x",
    "macd_pos", "macd_neg", "macd_cross_up",
    "bb_below_lower", "bb_below_mid",
    "prev_up", "prev_down", "prev_big_drop",
    "ret5d_neg", "ret5d_pos",
    "ret20d_neg", "ret20d_strong_neg", "ret20d_very_neg",
    "gap_up", "gap_up_big",
    "atr_expanding", "atr_strongly_expanding",
    "price_below_sma5", "price_below_sma20",
    "sma5_below_sma20", "golden_cross_near",
    "price_below_sma50", "price_below_sma200",
    "vol_3day_increase",
    "stoch_oversold", "williams_oversold",
]

NUM_CONDITIONS = len(CONDITION_NAMES)  # 36


# ---------------------------------------------------------------------------
# Indicator computation (from raw OHLCV)
# ---------------------------------------------------------------------------
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all technical indicators needed for the 36 conditions.
    Expects columns: Open, High, Low, Close, Volume (raw, not adjusted).
    Returns the same df with indicator columns appended.
    """
    c = df["Close"].astype(float)
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    o = df["Open"].astype(float)
    v = df["Volume"].astype(float)

    # --- Returns ---
    df["return_1d"] = c.pct_change(1)
    df["return_5d"] = c.pct_change(5)
    df["return_20d"] = c.pct_change(20)

    # --- Gap ---
    df["gap_pct"] = (o - c.shift(1)) / c.shift(1)

    # --- Volatility (20d log-return std) ---
    log_ret = np.log(c / c.shift(1))
    df["volatility_20d"] = log_ret.rolling(20).std()

    # --- RSI 14 (Wilder) ---
    df["rsi_14"] = calc_rsi_wilder(c, period=14)

    # --- 52-week high distance ---
    high_52w = h.rolling(252, min_periods=126).max()
    df["dist_52w_high"] = (c - high_52w) / high_52w

    # --- Volume ratio (vs 20d avg) ---
    vol_avg_20 = v.rolling(20).mean()
    df["vol_ratio"] = v / vol_avg_20.replace(0, np.nan)

    # --- MACD ---
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = macd_line - signal_line
    df["macd_hist_prev"] = df["macd_hist"].shift(1)

    # --- Bollinger Bands %B ---
    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    upper = sma20 + 2 * std20
    lower = sma20 - 2 * std20
    df["bb_pctb"] = (c - lower) / (upper - lower).replace(0, np.nan)

    # --- SMAs ---
    df["sma_5"] = c.rolling(5).mean()
    df["sma_20"] = sma20
    df["sma_50"] = c.rolling(50).mean()
    df["sma_200"] = c.rolling(200).mean()

    # --- ATR 14 and change ---
    tr = pd.concat([
        h - l,
        (h - c.shift(1)).abs(),
        (l - c.shift(1)).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()
    df["atr_change_5d"] = df["atr_14"].pct_change(5)

    # --- Stochastic K (14-period) ---
    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    df["stoch_k"] = 100 * (c - low14) / (high14 - low14).replace(0, np.nan)

    # --- Williams %R (14-period) ---
    df["williams_r"] = -100 * (high14 - c) / (high14 - low14).replace(0, np.nan)

    return df


# ---------------------------------------------------------------------------
# Evaluate 36 boolean conditions (vectorized)
# ---------------------------------------------------------------------------
def evaluate_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a DataFrame of 36 boolean columns, one per condition.
    Any NaN indicator values result in False for that condition.
    """
    conds = pd.DataFrame(index=df.index)
    c = df["Close"].astype(float)

    conds["high_vol"] = df["volatility_20d"] > 0.06
    conds["very_high_vol"] = df["volatility_20d"] > 0.10

    conds["rsi_below_30"] = df["rsi_14"] < 30
    conds["rsi_below_40"] = df["rsi_14"] < 40
    conds["rsi_30_50"] = (df["rsi_14"] >= 30) & (df["rsi_14"] <= 50)

    conds["near_52w_low"] = df["dist_52w_high"] < -0.70
    conds["deep_52w_low"] = df["dist_52w_high"] < -0.85

    conds["vol_1_5x"] = df["vol_ratio"] > 1.5
    conds["vol_2x"] = df["vol_ratio"] > 2.0
    conds["vol_3x"] = df["vol_ratio"] > 3.0

    conds["macd_pos"] = df["macd_hist"] > 0
    conds["macd_neg"] = df["macd_hist"] < 0
    conds["macd_cross_up"] = (df["macd_hist"] > 0) & (df["macd_hist_prev"] < 0)

    conds["bb_below_lower"] = df["bb_pctb"] < 0
    conds["bb_below_mid"] = df["bb_pctb"] < 0.5

    conds["prev_up"] = df["return_1d"] > 0
    conds["prev_down"] = df["return_1d"] < 0
    conds["prev_big_drop"] = df["return_1d"] < -0.05

    conds["ret5d_neg"] = df["return_5d"] < 0
    conds["ret5d_pos"] = df["return_5d"] > 0

    conds["ret20d_neg"] = df["return_20d"] < 0
    conds["ret20d_strong_neg"] = df["return_20d"] < -0.15
    conds["ret20d_very_neg"] = df["return_20d"] < -0.30

    conds["gap_up"] = df["gap_pct"] > 0.02
    conds["gap_up_big"] = df["gap_pct"] > 0.05

    conds["atr_expanding"] = df["atr_change_5d"] > 0.10
    conds["atr_strongly_expanding"] = df["atr_change_5d"] > 0.25

    conds["price_below_sma5"] = c < df["sma_5"]
    conds["price_below_sma20"] = c < df["sma_20"]
    conds["sma5_below_sma20"] = df["sma_5"] < df["sma_20"]

    sma5 = df["sma_5"]
    sma20 = df["sma_20"]
    ratio = (sma5 - sma20) / sma20.replace(0, np.nan)
    conds["golden_cross_near"] = (ratio > -0.02) & (ratio < 0.02)

    conds["price_below_sma50"] = c < df["sma_50"]
    conds["price_below_sma200"] = c < df["sma_200"]

    vol = df["Volume"].astype(float)
    conds["vol_3day_increase"] = (vol > vol.shift(1)) & (vol.shift(1) > vol.shift(2))

    conds["stoch_oversold"] = df["stoch_k"] < 20
    conds["williams_oversold"] = df["williams_r"] < -80

    # NaN -> False
    conds = conds.fillna(False).astype(bool)

    # Ensure column order matches CONDITION_NAMES
    for cn in CONDITION_NAMES:
        if cn not in conds.columns:
            conds[cn] = False
    conds = conds[CONDITION_NAMES]

    return conds


# ---------------------------------------------------------------------------
# Tracker-compatible position evaluation
# ---------------------------------------------------------------------------
def evaluate_signal_tracker(
    entry_price: float,
    tp_pct: float,
    max_hold: int,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    signal_idx: int,
    n_rows: int,
) -> str:
    """
    Evaluate a single signal using tracker.py's exact rules.

    Args:
        entry_price: Close price on signal day.
        tp_pct: TP threshold (e.g. 0.10 for +10%).
        max_hold: Maximum holding days.
        highs, lows, closes: numpy arrays for the entire price series.
        signal_idx: index of the signal day in the arrays.
        n_rows: total length of arrays.

    Returns:
        'WIN', 'LOSS', or 'EXPIRED'
    """
    # TP price: floor to 2 decimals (matches scanner's floor2)
    tp_price = math.floor(entry_price * (1 + tp_pct) * 100) / 100
    # SL price: round to 2 decimals, always -20%
    sl_price = round(entry_price * (1 + SL_PCT), 2)

    # Tracking starts from D+1
    start_idx = signal_idx + 1
    end_idx = min(signal_idx + 1 + max_hold, n_rows)

    if start_idx >= n_rows:
        return "EXPIRED"

    for day_i, idx in enumerate(range(start_idx, end_idx)):
        h = highs[idx]
        lo = lows[idx]
        c_val = closes[idx]

        if np.isnan(h) or np.isnan(lo):
            continue

        tp_hit = h >= tp_price
        sl_hit = lo <= sl_price

        # Same day TP+SL -> LOSS (conservative)
        if tp_hit and sl_hit:
            return "LOSS"

        if tp_hit:
            return "WIN"

        if sl_hit:
            return "LOSS"

        # Check if we've reached max_hold days
        days_held = day_i + 1
        if days_held >= max_hold:
            return "EXPIRED"

    return "EXPIRED"


# ---------------------------------------------------------------------------
# Download and prepare data for all tickers
# ---------------------------------------------------------------------------
def download_all_data():
    """
    Download 5 years of daily OHLCV for all US tickers.
    Returns dict: ticker -> DataFrame with indicators + conditions computed.
    """
    print("[1/3] Fetching ticker list...", flush=True)
    all_tickers = get_all_tickers()

    # Filter out warrants (ending in W) and leveraged ETFs (already done in get_all_tickers)
    tickers = [t for t in all_tickers if not t.endswith("W")]
    print(f"  {len(tickers)} tickers after filtering", flush=True)

    print("[2/3] Downloading 5 years of price data...", flush=True)
    ticker_data = {}
    n_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    total_loaded = 0
    total_skipped = 0

    for batch_i in range(n_batches):
        start = batch_i * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(tickers))
        batch_tickers = tickers[start:end]
        batch_len = len(batch_tickers)

        data = download_batch(batch_tickers, period="5y")

        for tk in batch_tickers:
            try:
                tkdf = extract_ticker_df(data, tk, batch_len)
                if tkdf is None or len(tkdf) < 252:
                    total_skipped += 1
                    continue

                # Ensure we have needed columns
                needed = {"Open", "High", "Low", "Close", "Volume"}
                if not needed.issubset(set(tkdf.columns)):
                    total_skipped += 1
                    continue

                tkdf = tkdf.copy()
                tkdf["Close"] = tkdf["Close"].astype(float)
                tkdf["Volume"] = tkdf["Volume"].astype(float)

                # Filter: price >= $1, avg volume >= 10000
                if tkdf["Close"].median() < 1.0:
                    total_skipped += 1
                    continue
                if tkdf["Volume"].rolling(20).mean().iloc[-1] < 10000:
                    total_skipped += 1
                    continue

                # Compute indicators
                tkdf = compute_indicators(tkdf)

                # Evaluate conditions
                conds = evaluate_conditions(tkdf)

                ticker_data[tk] = {
                    "close": tkdf["Close"].values.astype(np.float64),
                    "high": tkdf["High"].values.astype(np.float64),
                    "low": tkdf["Low"].values.astype(np.float64),
                    "conds": conds.values,  # bool array (n_days, 36)
                    "n_rows": len(tkdf),
                }
                total_loaded += 1

            except Exception:
                total_skipped += 1
                continue

        if (batch_i + 1) % 10 == 0 or batch_i == n_batches - 1:
            print(
                f"  Batch {batch_i+1}/{n_batches}: "
                f"loaded={total_loaded}, skipped={total_skipped}",
                flush=True,
            )

        if batch_i < n_batches - 1:
            time.sleep(BATCH_DELAY)

    print(
        f"  Done: {total_loaded} tickers loaded, {total_skipped} skipped",
        flush=True,
    )
    return ticker_data


# ---------------------------------------------------------------------------
# Build combined signal/price arrays across all tickers
# ---------------------------------------------------------------------------
def build_combined_arrays(ticker_data: dict):
    """
    Flatten all tickers into combined arrays for fast combo evaluation.

    Returns:
        cond_matrix: (N, 36) bool numpy array
        close_arr, high_arr, low_arr: (N,) float64 arrays
        valid_mask: (N,) bool - True for rows where we can evaluate (enough future data)
        signal_info: list of (ticker, local_idx) for each row — kept for debugging only
                     but we skip building this for memory efficiency.
        row_start_per_ticker: offsets so we know which rows belong to each ticker.
                              Not needed if we precompute with enough trailing room.
    """
    # We need the condition matrix and price arrays.
    # But for tracker evaluation, each signal day needs FUTURE price data from
    # the SAME ticker. So we cannot simply concatenate and lose ticker boundaries.
    #
    # Strategy: keep per-ticker arrays but build a combined condition matrix
    # for fast combo signal detection, along with metadata to map back.

    all_conds = []
    all_close = []
    all_high = []
    all_low = []
    # (global_offset, n_rows) per ticker for mapping
    ticker_ranges = []

    offset = 0
    for tk, td in ticker_data.items():
        n = td["n_rows"]
        all_conds.append(td["conds"])
        all_close.append(td["close"])
        all_high.append(td["high"])
        all_low.append(td["low"])
        ticker_ranges.append((offset, n))
        offset += n

    cond_matrix = np.concatenate(all_conds, axis=0)
    close_arr = np.concatenate(all_close)
    high_arr = np.concatenate(all_high)
    low_arr = np.concatenate(all_low)

    return cond_matrix, close_arr, high_arr, low_arr, ticker_ranges


# ---------------------------------------------------------------------------
# Evaluate a combo of conditions
# ---------------------------------------------------------------------------
def evaluate_combo(
    combo_indices: tuple,
    cond_matrix: np.ndarray,
    close_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    ticker_ranges: list,
    tp_pct: float,
    max_hold: int,
    min_signals: int,
):
    """
    Given a combo of condition column indices, find signal days across all tickers,
    evaluate each using tracker rules, and return (wins, losses, expired, total_signals).

    Returns None if not enough signals.
    """
    # Build signal mask: all conditions in combo must be True
    signal_mask = cond_matrix[:, combo_indices[0]]
    for ci in combo_indices[1:]:
        signal_mask = signal_mask & cond_matrix[:, ci]

    total_signals = signal_mask.sum()
    if total_signals < min_signals:
        return None

    wins = 0
    losses = 0
    expired = 0

    # For each ticker, find its signals and evaluate
    for (g_offset, n_rows) in ticker_ranges:
        # Signal indices within this ticker
        tk_mask = signal_mask[g_offset : g_offset + n_rows]
        local_indices = np.where(tk_mask)[0]

        if len(local_indices) == 0:
            continue

        # Get price arrays for this ticker
        tk_close = close_arr[g_offset : g_offset + n_rows]
        tk_high = high_arr[g_offset : g_offset + n_rows]
        tk_low = low_arr[g_offset : g_offset + n_rows]

        for sig_idx in local_indices:
            entry_price = tk_close[sig_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                expired += 1
                continue

            result = evaluate_signal_tracker(
                entry_price, tp_pct, max_hold,
                tk_high, tk_low, tk_close,
                sig_idx, n_rows,
            )

            if result == "WIN":
                wins += 1
            elif result == "LOSS":
                losses += 1
            else:
                expired += 1

    return wins, losses, expired, int(total_signals)


# ---------------------------------------------------------------------------
# Search combos for a given (thresh, period, combo_size)
# ---------------------------------------------------------------------------
def search_combos(
    combo_size: int,
    cond_matrix: np.ndarray,
    close_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    ticker_ranges: list,
    tp_pct: float,
    max_hold: int,
    min_signals: int,
    min_precision: float,
):
    """
    Search all combinations of `combo_size` conditions.
    Returns list of result dicts for combos meeting min_precision.
    """
    n_conds = cond_matrix.shape[1]
    total_combos = 1
    for i in range(combo_size):
        total_combos = total_combos * (n_conds - i) // (i + 1)

    print(
        f"\n  Searching {combo_size}-condition combos: {total_combos:,} total",
        flush=True,
    )

    results = []
    checked = 0
    start_t = time.time()

    for combo in combinations(range(n_conds), combo_size):
        checked += 1

        ret = evaluate_combo(
            combo, cond_matrix, close_arr, high_arr, low_arr,
            ticker_ranges, tp_pct, max_hold, min_signals,
        )

        if ret is not None:
            w, l, e, total = ret
            if total > 0:
                precision = w / total * 100
                if precision >= min_precision:
                    cond_str = " + ".join(CONDITION_NAMES[i] for i in combo)
                    results.append({
                        "thresh": tp_pct,
                        "period": max_hold,
                        "combo_size": combo_size,
                        "conditions": cond_str,
                        "precision": round(precision, 1),
                        "signals": total,
                        "wins": w,
                        "losses": l,
                        "expired": e,
                    })

        if checked % 1000 == 0:
            elapsed = time.time() - start_t
            pct = checked / total_combos * 100
            speed = checked / max(elapsed, 0.01)
            eta = (total_combos - checked) / max(speed, 1)
            print(
                f"    {checked:,}/{total_combos:,} ({pct:.1f}%) "
                f"| found={len(results)} "
                f"| {elapsed:.0f}s elapsed, ~{eta:.0f}s remaining",
                flush=True,
            )

    elapsed = time.time() - start_t
    print(
        f"    Done: {checked:,} combos in {elapsed:.0f}s, "
        f"{len(results)} combos with >={min_precision}% precision",
        flush=True,
    )

    return results


# ---------------------------------------------------------------------------
# Run a single (thresh, period) configuration
# ---------------------------------------------------------------------------
def run_single(
    tp_pct: float,
    max_hold: int,
    combo_sizes: list[int],
    cond_matrix: np.ndarray,
    close_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    ticker_ranges: list,
    min_signals: int,
    min_precision: float,
):
    """Run combo search for one (thresh, period) and all requested combo sizes."""
    all_results = []

    for cs in combo_sizes:
        header = f"=== THRESH={tp_pct:.2f}, PERIOD={max_hold}, COMBO_SIZE={cs} ==="
        print(f"\n{header}", flush=True)

        results = search_combos(
            cs, cond_matrix, close_arr, high_arr, low_arr,
            ticker_ranges, tp_pct, max_hold, min_signals, min_precision,
        )

        # Sort: signals desc, precision desc
        results.sort(key=lambda r: (-r["signals"], -r["precision"]))

        # Print
        if results:
            print(
                f"\n{'Precision':>10}  {'Signals':>8}  {'Wins':>5}  "
                f"{'Losses':>7}  {'Expired':>8}  Conditions",
                flush=True,
            )
            for r in results:
                print(
                    f"{r['precision']:>9.1f}%  {r['signals']:>8}  "
                    f"{r['wins']:>5}  {r['losses']:>7}  {r['expired']:>8}  "
                    f"{r['conditions']}",
                    flush=True,
                )
            print(
                f"\nFound {len(results)} combos with >={min_precision}% precision",
                flush=True,
            )
        else:
            print(f"\nNo combos found with >={min_precision}% precision", flush=True)

        all_results.extend(results)

    # Save CSV
    if all_results:
        csv_name = f"backtest_v2_{tp_pct:.2f}_{max_hold}.csv"
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), csv_name)
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "thresh", "period", "combo_size", "conditions",
                    "precision", "signals", "wins", "losses", "expired",
                ],
            )
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nResults saved to {csv_path}", flush=True)

    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Tracker-compatible backtest: find high-precision condition combos"
    )
    parser.add_argument(
        "--thresh", type=float, default=None,
        help="TP threshold (0.10, 0.15, 0.20, 0.30)",
    )
    parser.add_argument(
        "--period", type=int, default=None,
        help="Max hold days (3, 5, 7, 10, 15, 20)",
    )
    parser.add_argument(
        "--combo", type=int, default=None,
        help="Combo size (3, 4, or 5). Default: test all 3,4,5",
    )
    parser.add_argument(
        "--min-signals", type=int, default=100,
        help="Minimum signals required (default: 100)",
    )
    parser.add_argument(
        "--min-precision", type=float, default=90,
        help="Minimum win rate %% to report (default: 90)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run all 24 thresh x period combinations",
    )

    args = parser.parse_args()

    # Determine which (thresh, period) pairs to run
    if args.all:
        pairs = [(t, p) for t in ALL_THRESHOLDS for p in ALL_PERIODS]
    elif args.thresh is not None and args.period is not None:
        pairs = [(args.thresh, args.period)]
    else:
        parser.error("Specify --thresh and --period, or use --all")

    # Determine combo sizes
    if args.combo is not None:
        combo_sizes = [args.combo]
    else:
        combo_sizes = [3, 4, 5]

    print("=" * 70, flush=True)
    print("find_high_precision_v2.py — Tracker-Compatible Backtest", flush=True)
    print("=" * 70, flush=True)
    print(f"  Pairs to test  : {len(pairs)}", flush=True)
    print(f"  Combo sizes    : {combo_sizes}", flush=True)
    print(f"  Min signals    : {args.min_signals}", flush=True)
    print(f"  Min precision  : {args.min_precision}%", flush=True)
    print(f"  SL             : {SL_PCT*100:.0f}% (fixed)", flush=True)
    print(f"  TP floor rule  : math.floor(entry*(1+tp)*100)/100", flush=True)
    print(f"  Same-day TP+SL : LOSS (conservative)", flush=True)
    print(f"  Expired        : counts as failure", flush=True)
    print("=" * 70, flush=True)

    total_start = time.time()

    # Download and prepare data (once)
    ticker_data = download_all_data()

    if not ticker_data:
        print("ERROR: No ticker data loaded. Exiting.", flush=True)
        sys.exit(1)

    # Build combined arrays
    print("\n[3/3] Building combined arrays...", flush=True)
    cond_matrix, close_arr, high_arr, low_arr, ticker_ranges = build_combined_arrays(
        ticker_data
    )
    # Free per-ticker data to save memory
    del ticker_data

    total_rows = len(close_arr)
    print(f"  Total rows: {total_rows:,} across {len(ticker_ranges)} tickers", flush=True)

    # Run all requested (thresh, period) pairs
    grand_results = []

    for tp_pct, max_hold in pairs:
        pair_start = time.time()
        results = run_single(
            tp_pct, max_hold, combo_sizes,
            cond_matrix, close_arr, high_arr, low_arr, ticker_ranges,
            args.min_signals, args.min_precision,
        )
        grand_results.extend(results)
        pair_elapsed = time.time() - pair_start
        print(
            f"\n  [THRESH={tp_pct}, PERIOD={max_hold}] "
            f"completed in {pair_elapsed:.0f}s, {len(results)} results",
            flush=True,
        )

    # Final summary
    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 70}", flush=True)
    print("FINAL SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Total combos with >={args.min_precision}% precision: {len(grand_results)}", flush=True)
    print(f"  Total time: {total_elapsed/60:.1f} minutes", flush=True)

    if grand_results:
        # Save grand summary CSV
        grand_csv = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "backtest_v2_all_results.csv",
        )
        with open(grand_csv, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "thresh", "period", "combo_size", "conditions",
                    "precision", "signals", "wins", "losses", "expired",
                ],
            )
            writer.writeheader()
            # Sort by signals desc, precision desc
            grand_results.sort(key=lambda r: (-r["signals"], -r["precision"]))
            writer.writerows(grand_results)
        print(f"  Grand results saved to {grand_csv}", flush=True)

        # Print top 20
        print(f"\n  Top results (by signal count):", flush=True)
        print(
            f"  {'Thresh':>6}  {'Period':>6}  {'Prec':>6}  {'Sigs':>5}  "
            f"{'W':>4}  {'L':>4}  {'E':>4}  Conditions",
            flush=True,
        )
        for r in grand_results[:20]:
            print(
                f"  {r['thresh']:>6.2f}  {r['period']:>6}  "
                f"{r['precision']:>5.1f}%  {r['signals']:>5}  "
                f"{r['wins']:>4}  {r['losses']:>4}  {r['expired']:>4}  "
                f"{r['conditions']}",
                flush=True,
            )

    print(f"\n{'=' * 70}", flush=True)
    print("Done!", flush=True)
    print(f"{'=' * 70}", flush=True)


if __name__ == "__main__":
    main()
