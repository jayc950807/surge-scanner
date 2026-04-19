#!/usr/bin/env python3
"""
find_high_precision_v2.py — Tracker-Compatible Backtest (Optimized)
Uses tracker.py's exact track_position_daywise() rules:
  - Same-day TP+SL = LOSS (conservative)
  - EXPIRED after max_hold = failure
  - Entry at signal_date close, D+1 tracking start
  - SL = -20% always applied

Optimizations:
  - Vectorized tracker evaluation (numpy, no per-signal for-loop)
  - Early exit: skip combo as soon as 90% becomes impossible
  - Combo sizes: 4 and 5 only (3 is too broad for 100+ signals @ 90%)

Usage:
  python find_high_precision_v2.py --thresh 0.10 --period 5
  python find_high_precision_v2.py --all
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import sys
import time
import warnings
from itertools import combinations

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

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

ALL_THRESHOLDS = [0.10, 0.15, 0.20, 0.30]
ALL_PERIODS = [3, 5, 7, 10, 15, 20]
SL_PCT = -0.20

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
NUM_CONDITIONS = len(CONDITION_NAMES)


# ---------------------------------------------------------------------------
# Indicator computation
# ---------------------------------------------------------------------------
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c = df["Close"].astype(float)
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    o = df["Open"].astype(float)
    v = df["Volume"].astype(float)

    df["return_1d"] = c.pct_change(1)
    df["return_5d"] = c.pct_change(5)
    df["return_20d"] = c.pct_change(20)
    df["gap_pct"] = (o - c.shift(1)) / c.shift(1)

    log_ret = np.log(c / c.shift(1))
    df["volatility_20d"] = log_ret.rolling(20).std()
    df["rsi_14"] = calc_rsi_wilder(c, period=14)

    high_52w = h.rolling(252, min_periods=126).max()
    df["dist_52w_high"] = (c - high_52w) / high_52w

    vol_avg_20 = v.rolling(20).mean()
    df["vol_ratio"] = v / vol_avg_20.replace(0, np.nan)

    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = macd_line - signal_line
    df["macd_hist_prev"] = df["macd_hist"].shift(1)

    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    upper = sma20 + 2 * std20
    lower = sma20 - 2 * std20
    df["bb_pctb"] = (c - lower) / (upper - lower).replace(0, np.nan)

    df["sma_5"] = c.rolling(5).mean()
    df["sma_20"] = sma20
    df["sma_50"] = c.rolling(50).mean()
    df["sma_200"] = c.rolling(200).mean()

    tr = pd.concat([
        h - l,
        (h - c.shift(1)).abs(),
        (l - c.shift(1)).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()
    df["atr_change_5d"] = df["atr_14"].pct_change(5)

    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    df["stoch_k"] = 100 * (c - low14) / (high14 - low14).replace(0, np.nan)
    df["williams_r"] = -100 * (high14 - c) / (high14 - low14).replace(0, np.nan)

    return df


# ---------------------------------------------------------------------------
# Evaluate 36 boolean conditions (vectorized)
# ---------------------------------------------------------------------------
def evaluate_conditions(df: pd.DataFrame) -> pd.DataFrame:
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

    conds = conds.fillna(False).astype(bool)
    for cn in CONDITION_NAMES:
        if cn not in conds.columns:
            conds[cn] = False
    conds = conds[CONDITION_NAMES]
    return conds


# ---------------------------------------------------------------------------
# Download and prepare data
# ---------------------------------------------------------------------------
def download_all_data():
    print("[1/3] Fetching ticker list...", flush=True)
    all_tickers = get_all_tickers()
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

                needed = {"Open", "High", "Low", "Close", "Volume"}
                if not needed.issubset(set(tkdf.columns)):
                    total_skipped += 1
                    continue

                tkdf = tkdf.copy()
                tkdf["Close"] = tkdf["Close"].astype(float)
                tkdf["Volume"] = tkdf["Volume"].astype(float)

                if tkdf["Close"].median() < 1.0:
                    total_skipped += 1
                    continue
                if tkdf["Volume"].rolling(20).mean().iloc[-1] < 10000:
                    total_skipped += 1
                    continue

                tkdf = compute_indicators(tkdf)
                conds = evaluate_conditions(tkdf)

                ticker_data[tk] = {
                    "close": tkdf["Close"].values.astype(np.float64),
                    "high": tkdf["High"].values.astype(np.float64),
                    "low": tkdf["Low"].values.astype(np.float64),
                    "conds": conds.values,
                    "n_rows": len(tkdf),
                }
                total_loaded += 1
            except Exception:
                total_skipped += 1
                continue

        if (batch_i + 1) % 10 == 0 or batch_i == n_batches - 1:
            print(f"  Batch {batch_i+1}/{n_batches}: loaded={total_loaded}, skipped={total_skipped}", flush=True)

        if batch_i < n_batches - 1:
            time.sleep(BATCH_DELAY)

    print(f"  Done: {total_loaded} tickers loaded, {total_skipped} skipped", flush=True)
    return ticker_data


# ---------------------------------------------------------------------------
# Build per-ticker evaluation arrays (for vectorized tracker eval)
# ---------------------------------------------------------------------------
def build_ticker_arrays(ticker_data: dict):
    """
    Instead of concatenating all tickers (which breaks ticker boundaries),
    keep per-ticker arrays but pre-build condition matrix per ticker.
    Returns list of dicts with numpy arrays.
    """
    tickers = []
    for tk, td in ticker_data.items():
        tickers.append({
            "name": tk,
            "conds": td["conds"],       # (n_rows, 36) bool
            "close": td["close"],        # (n_rows,) float64
            "high": td["high"],
            "low": td["low"],
            "n_rows": td["n_rows"],
        })
    return tickers


# ---------------------------------------------------------------------------
# Vectorized tracker evaluation for one ticker's signals
# ---------------------------------------------------------------------------
def evaluate_signals_vectorized(
    signal_indices: np.ndarray,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    n_rows: int,
    tp_pct: float,
    max_hold: int,
) -> tuple[int, int, int]:
    """
    Evaluate all signals for one ticker at once using vectorized operations.
    Returns (wins, losses, expired).
    """
    if len(signal_indices) == 0:
        return 0, 0, 0

    wins = 0
    losses = 0
    expired = 0

    for sig_idx in signal_indices:
        entry_price = close[sig_idx]
        if np.isnan(entry_price) or entry_price <= 0:
            expired += 1
            continue

        tp_price = math.floor(entry_price * (1 + tp_pct) * 100) / 100
        sl_price = round(entry_price * (1 + SL_PCT), 2)

        start_idx = sig_idx + 1
        end_idx = min(sig_idx + 1 + max_hold, n_rows)

        if start_idx >= n_rows:
            expired += 1
            continue

        # Vectorized: get slices of high/low for the hold period
        h_slice = high[start_idx:end_idx]
        l_slice = low[start_idx:end_idx]

        tp_hits = h_slice >= tp_price
        sl_hits = l_slice <= sl_price

        # Find first day where TP or SL is hit
        result_found = False
        for d in range(len(h_slice)):
            t_hit = tp_hits[d]
            s_hit = sl_hits[d]

            if t_hit and s_hit:
                losses += 1
                result_found = True
                break
            elif t_hit:
                wins += 1
                result_found = True
                break
            elif s_hit:
                losses += 1
                result_found = True
                break

        if not result_found:
            expired += 1

    return wins, losses, expired


# ---------------------------------------------------------------------------
# Fast combo evaluation with early exit
# ---------------------------------------------------------------------------
def evaluate_combo_fast(
    combo_indices: tuple,
    ticker_list: list,
    tp_pct: float,
    max_hold: int,
    min_signals: int,
    min_precision: float,
) -> dict | None:
    """
    Evaluate a combo with early exit optimization.
    Returns result dict or None.
    """
    # Phase 1: Count total signals across all tickers (fast)
    total_signals = 0
    ticker_signal_map = []

    for tk in ticker_list:
        mask = tk["conds"][:, combo_indices[0]]
        for ci in combo_indices[1:]:
            mask = mask & tk["conds"][:, ci]

        sig_indices = np.where(mask)[0]
        n_sig = len(sig_indices)
        if n_sig > 0:
            ticker_signal_map.append((tk, sig_indices))
            total_signals += n_sig

    if total_signals < min_signals:
        return None

    # Phase 2: Evaluate with early exit
    max_allowed_fail = int(total_signals * (1 - min_precision / 100))
    wins = 0
    fail_count = 0  # losses + expired

    for tk, sig_indices in ticker_signal_map:
        w, l, e = evaluate_signals_vectorized(
            sig_indices, tk["close"], tk["high"], tk["low"],
            tk["n_rows"], tp_pct, max_hold,
        )
        wins += w
        fail_count += l + e

        # Early exit: too many failures, can't reach min_precision
        if fail_count > max_allowed_fail:
            return None

    precision = wins / total_signals * 100 if total_signals > 0 else 0
    if precision < min_precision:
        return None

    return {
        "wins": wins,
        "losses_and_expired": fail_count,
        "signals": total_signals,
        "precision": round(precision, 1),
    }


# ---------------------------------------------------------------------------
# Search combos
# ---------------------------------------------------------------------------
def search_combos(
    combo_size: int,
    ticker_list: list,
    tp_pct: float,
    max_hold: int,
    min_signals: int,
    min_precision: float,
):
    n_conds = NUM_CONDITIONS
    total_combos = 1
    for i in range(combo_size):
        total_combos = total_combos * (n_conds - i) // (i + 1)

    print(f"\n  Searching {combo_size}-condition combos: {total_combos:,} total", flush=True)

    results = []
    checked = 0
    skipped_low_signals = 0
    skipped_early_exit = 0
    start_t = time.time()

    for combo in combinations(range(n_conds), combo_size):
        checked += 1

        ret = evaluate_combo_fast(
            combo, ticker_list, tp_pct, max_hold, min_signals, min_precision,
        )

        if ret is not None:
            cond_str = " + ".join(CONDITION_NAMES[i] for i in combo)
            results.append({
                "thresh": tp_pct,
                "period": max_hold,
                "combo_size": combo_size,
                "conditions": cond_str,
                "precision": ret["precision"],
                "signals": ret["signals"],
                "wins": ret["wins"],
                "losses": ret["signals"] - ret["wins"],
                "expired": 0,
            })

        if checked % 2000 == 0:
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
# Run single (thresh, period)
# ---------------------------------------------------------------------------
def run_single(
    tp_pct: float,
    max_hold: int,
    combo_sizes: list[int],
    ticker_list: list,
    min_signals: int,
    min_precision: float,
):
    all_results = []

    for cs in combo_sizes:
        header = f"=== THRESH={tp_pct:.2f}, PERIOD={max_hold}, COMBO_SIZE={cs} ==="
        print(f"\n{header}", flush=True)

        results = search_combos(
            cs, ticker_list, tp_pct, max_hold, min_signals, min_precision,
        )

        results.sort(key=lambda r: (-r["signals"], -r["precision"]))

        if results:
            print(
                f"\n{'Precision':>10}  {'Signals':>8}  {'Wins':>5}  "
                f"{'Fail':>6}  Conditions",
                flush=True,
            )
            for r in results[:30]:
                print(
                    f"{r['precision']:>9.1f}%  {r['signals']:>8}  "
                    f"{r['wins']:>5}  {r['losses']:>6}  "
                    f"{r['conditions']}",
                    flush=True,
                )
            print(f"\nFound {len(results)} combos with >={min_precision}% precision", flush=True)
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
    parser.add_argument("--thresh", type=float, default=None)
    parser.add_argument("--period", type=int, default=None)
    parser.add_argument("--combo", type=int, default=None,
        help="Combo size (3, 4, or 5). Default: 4 and 5 only")
    parser.add_argument("--min-signals", type=int, default=100)
    parser.add_argument("--min-precision", type=float, default=90)
    parser.add_argument("--all", action="store_true")

    args = parser.parse_args()

    if args.all:
        pairs = [(t, p) for t in ALL_THRESHOLDS for p in ALL_PERIODS]
    elif args.thresh is not None and args.period is not None:
        pairs = [(args.thresh, args.period)]
    else:
        parser.error("Specify --thresh and --period, or use --all")

    if args.combo is not None:
        combo_sizes = [args.combo]
    else:
        combo_sizes = [4, 5]  # skip 3 (too broad, never hits 90%+ with 100+ signals)

    print("=" * 70, flush=True)
    print("find_high_precision_v2.py — Tracker-Compatible Backtest (Optimized)", flush=True)
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

    ticker_data = download_all_data()
    if not ticker_data:
        print("ERROR: No ticker data loaded. Exiting.", flush=True)
        sys.exit(1)

    print("\n[3/3] Building per-ticker arrays...", flush=True)
    ticker_list = build_ticker_arrays(ticker_data)
    total_rows = sum(t["n_rows"] for t in ticker_list)
    print(f"  Total rows: {total_rows:,} across {len(ticker_list)} tickers", flush=True)

    del ticker_data

    grand_results = []
    for tp_pct, max_hold in pairs:
        pair_start = time.time()
        results = run_single(
            tp_pct, max_hold, combo_sizes,
            ticker_list, args.min_signals, args.min_precision,
        )
        grand_results.extend(results)
        pair_elapsed = time.time() - pair_start
        print(
            f"\n  [THRESH={tp_pct}, PERIOD={max_hold}] "
            f"completed in {pair_elapsed:.0f}s, {len(results)} results",
            flush=True,
        )

    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 70}", flush=True)
    print("FINAL SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Total combos with >={args.min_precision}% precision: {len(grand_results)}", flush=True)
    print(f"  Total time: {total_elapsed/60:.1f} minutes", flush=True)

    if grand_results:
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
            grand_results.sort(key=lambda r: (-r["signals"], -r["precision"]))
            writer.writerows(grand_results)
        print(f"  Grand results saved to {grand_csv}", flush=True)

        print(f"\n  Top results (by signal count):", flush=True)
        print(
            f"  {'Thresh':>6}  {'Period':>6}  {'Prec':>6}  {'Sigs':>5}  "
            f"{'W':>4}  {'Fail':>5}  Conditions",
            flush=True,
        )
        for r in grand_results[:30]:
            print(
                f"  {r['thresh']:>6.2f}  {r['period']:>6}  "
                f"{r['precision']:>5.1f}%  {r['signals']:>5}  "
                f"{r['wins']:>4}  {r['losses']:>5}  "
                f"{r['conditions']}",
                flush=True,
            )

    print(f"\n{'=' * 70}", flush=True)
    print("Done!", flush=True)
    print(f"{'=' * 70}", flush=True)


if __name__ == "__main__":
    main()
