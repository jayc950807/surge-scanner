#!/usr/bin/env python3
"""
find_high_precision_v2.py — Tracker-Compatible Backtest (2-Phase Optimized)

Phase 1: numpy AND on concatenated 8.7M-row matrix → count signals per combo
          (~0.5ms per combo, 435K combos in ~4 min). Skip combos with < 100 signals.
Phase 2: Only combos passing Phase 1 get tracker evaluation (numba JIT).
          Early exit if failures exceed threshold.

Tracker rules (same as tracker.py):
  - Same-day TP+SL = LOSS
  - EXPIRED after max_hold = failure
  - Entry at signal_date close, D+1 tracking
  - SL = -20%, TP = floor(entry*(1+tp)*100)/100
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
from numba import njit

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


# ═══════════════════════════════════════════════════════════════════════════
# Numba: evaluate signals for one ticker
# ═══════════════════════════════════════════════════════════════════════════
@njit(cache=True)
def _eval_ticker_signals(signal_indices, close, high, low, n_rows, tp_pct, sl_pct, max_hold):
    wins = 0
    failures = 0
    for i in range(len(signal_indices)):
        sig_idx = signal_indices[i]
        entry = close[sig_idx]
        if entry != entry or entry <= 0:
            failures += 1
            continue
        tp_price = math.floor(entry * (1.0 + tp_pct) * 100.0) / 100.0
        sl_price = round(entry * (1.0 + sl_pct), 2)
        start = sig_idx + 1
        end = sig_idx + 1 + max_hold
        if end > n_rows:
            end = n_rows
        if start >= n_rows:
            failures += 1
            continue
        found = False
        for idx in range(start, end):
            h = high[idx]
            lo = low[idx]
            if h != h or lo != lo:
                continue
            if h >= tp_price and lo <= sl_price:
                failures += 1
                found = True
                break
            elif h >= tp_price:
                wins += 1
                found = True
                break
            elif lo <= sl_price:
                failures += 1
                found = True
                break
        if not found:
            failures += 1
    return wins, failures


# ═══════════════════════════════════════════════════════════════════════════
# Indicators & Conditions
# ═══════════════════════════════════════════════════════════════════════════
def compute_indicators(df):
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
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()
    df["atr_change_5d"] = df["atr_14"].pct_change(5)
    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    df["stoch_k"] = 100 * (c - low14) / (high14 - low14).replace(0, np.nan)
    df["williams_r"] = -100 * (high14 - c) / (high14 - low14).replace(0, np.nan)
    return df


def evaluate_conditions(df):
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


# ═══════════════════════════════════════════════════════════════════════════
# Download
# ═══════════════════════════════════════════════════════════════════════════
def download_all_data():
    print("[1/4] Fetching ticker list...", flush=True)
    all_tickers = get_all_tickers()
    tickers = [t for t in all_tickers if not t.endswith("W")]
    print(f"  {len(tickers)} tickers after filtering", flush=True)

    print("[2/4] Downloading 5 years of price data...", flush=True)
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


# ═══════════════════════════════════════════════════════════════════════════
# Build combined arrays (concatenated for Phase 1, per-ticker for Phase 2)
# ═══════════════════════════════════════════════════════════════════════════
def build_arrays(ticker_data):
    """
    Returns:
      cond_cols: list of 36 numpy bool arrays, each (N,) — for fast Phase 1 counting
      close_arr, high_arr, low_arr: (N,) float64 — concatenated price arrays
      ticker_ranges: list of (offset, n_rows) — ticker boundaries
    """
    all_conds = []
    all_close = []
    all_high = []
    all_low = []
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

    cond_matrix = np.concatenate(all_conds, axis=0)  # (N, 36) bool
    # Pre-split into per-column arrays for faster AND
    cond_cols = [cond_matrix[:, i].copy() for i in range(NUM_CONDITIONS)]

    close_arr = np.concatenate(all_close)
    high_arr = np.concatenate(all_high)
    low_arr = np.concatenate(all_low)

    return cond_cols, close_arr, high_arr, low_arr, ticker_ranges


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1: Fast signal counting (numpy only, no tracker eval)
# ═══════════════════════════════════════════════════════════════════════════
def phase1_count_signals(combo_size, cond_cols, min_signals):
    """
    For ALL combos of given size, count signals using numpy AND.
    Returns list of (combo_indices, signal_count) for combos with >= min_signals.
    """
    n_conds = NUM_CONDITIONS
    total_combos = 1
    for i in range(combo_size):
        total_combos = total_combos * (n_conds - i) // (i + 1)

    print(f"\n  Phase 1: Counting signals for {total_combos:,} {combo_size}-combos...", flush=True)

    # Pre-filter: skip conditions with very few True values
    cond_counts = np.array([c.sum() for c in cond_cols])
    print(f"    Per-condition signal counts: min={cond_counts.min():,}, max={cond_counts.max():,}", flush=True)

    # Precompute pair counts for pruning (36×36 = 1296 pairs)
    pair_counts = np.zeros((n_conds, n_conds), dtype=np.int64)
    for i in range(n_conds):
        for j in range(i + 1, n_conds):
            pair_counts[i, j] = (cond_cols[i] & cond_cols[j]).sum()
            pair_counts[j, i] = pair_counts[i, j]

    candidates = []
    checked = 0
    skipped_pair = 0
    start_t = time.time()

    for combo in combinations(range(n_conds), combo_size):
        checked += 1

        # Quick pair-pruning: if any pair in combo has < min_signals, skip
        skip = False
        for pi in range(len(combo)):
            for pj in range(pi + 1, len(combo)):
                if pair_counts[combo[pi], combo[pj]] < min_signals:
                    skip = True
                    break
            if skip:
                break

        if skip:
            skipped_pair += 1
            continue

        # Full count: AND all columns
        mask = cond_cols[combo[0]]
        for ci in combo[1:]:
            mask = mask & cond_cols[ci]
        count = mask.sum()

        if count >= min_signals:
            candidates.append((combo, int(count)))

        if checked % 20000 == 0:
            elapsed = time.time() - start_t
            pct = checked / total_combos * 100
            print(
                f"    {checked:,}/{total_combos:,} ({pct:.0f}%) "
                f"| candidates={len(candidates)} | skipped_pair={skipped_pair:,} "
                f"| {elapsed:.0f}s",
                flush=True,
            )

    elapsed = time.time() - start_t
    print(
        f"    Phase 1 done: {checked:,} checked, {skipped_pair:,} pair-pruned, "
        f"{len(candidates)} candidates in {elapsed:.0f}s",
        flush=True,
    )
    return candidates


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2: Tracker evaluation (numba, only for candidates)
# ═══════════════════════════════════════════════════════════════════════════
def phase2_evaluate(
    candidates, cond_cols, close_arr, high_arr, low_arr,
    ticker_ranges, tp_pct, max_hold, min_precision,
):
    """
    Evaluate only candidate combos using tracker rules.
    Returns list of result dicts.
    """
    if not candidates:
        return []

    print(f"\n  Phase 2: Evaluating {len(candidates)} candidates with tracker rules...", flush=True)

    results = []
    max_fail_ratio = 1.0 - min_precision / 100.0
    start_t = time.time()

    for ci, (combo, total_signals) in enumerate(candidates):
        # Build signal mask
        mask = cond_cols[combo[0]]
        for c_idx in combo[1:]:
            mask = mask & cond_cols[c_idx]

        max_allowed_fail = int(total_signals * max_fail_ratio)
        total_wins = 0
        total_fail = 0
        aborted = False

        for (g_offset, n_rows) in ticker_ranges:
            tk_mask = mask[g_offset: g_offset + n_rows]
            sig_indices = np.where(tk_mask)[0].astype(np.int64)
            if len(sig_indices) == 0:
                continue

            tk_close = close_arr[g_offset: g_offset + n_rows]
            tk_high = high_arr[g_offset: g_offset + n_rows]
            tk_low = low_arr[g_offset: g_offset + n_rows]

            w, f = _eval_ticker_signals(
                sig_indices, tk_close, tk_high, tk_low,
                n_rows, tp_pct, SL_PCT, max_hold,
            )
            total_wins += w
            total_fail += f

            if total_fail > max_allowed_fail:
                aborted = True
                break

        if aborted:
            continue

        precision = total_wins / total_signals * 100 if total_signals > 0 else 0
        if precision >= min_precision:
            cond_str = " + ".join(CONDITION_NAMES[i] for i in combo)
            results.append({
                "thresh": tp_pct,
                "period": max_hold,
                "combo_size": len(combo),
                "conditions": cond_str,
                "precision": round(precision, 1),
                "signals": total_signals,
                "wins": int(total_wins),
                "losses": int(total_fail),
                "expired": 0,
            })

        if (ci + 1) % 100 == 0 or ci == len(candidates) - 1:
            elapsed = time.time() - start_t
            print(
                f"    {ci+1}/{len(candidates)} evaluated | found={len(results)} | {elapsed:.0f}s",
                flush=True,
            )

    elapsed = time.time() - start_t
    print(f"    Phase 2 done: {len(results)} combos passed in {elapsed:.0f}s", flush=True)
    return results


# ═══════════════════════════════════════════════════════════════════════════
# Run single (thresh, period)
# ═══════════════════════════════════════════════════════════════════════════
def run_single(
    tp_pct, max_hold, combo_sizes,
    cond_cols, close_arr, high_arr, low_arr, ticker_ranges,
    min_signals, min_precision,
):
    all_results = []

    for cs in combo_sizes:
        print(f"\n{'='*60}", flush=True)
        print(f"  THRESH={tp_pct:.2f}, PERIOD={max_hold}, COMBO_SIZE={cs}", flush=True)
        print(f"{'='*60}", flush=True)

        # Phase 1: fast counting
        candidates = phase1_count_signals(cs, cond_cols, min_signals)

        # Phase 2: tracker evaluation
        results = phase2_evaluate(
            candidates, cond_cols, close_arr, high_arr, low_arr,
            ticker_ranges, tp_pct, max_hold, min_precision,
        )

        results.sort(key=lambda r: (-r["signals"], -r["precision"]))

        if results:
            print(f"\n  {'Prec':>7}  {'Sigs':>6}  {'Win':>5}  {'Fail':>5}  Conditions", flush=True)
            for r in results[:30]:
                print(f"  {r['precision']:>6.1f}%  {r['signals']:>6}  {r['wins']:>5}  {r['losses']:>5}  {r['conditions']}", flush=True)
        else:
            print(f"\n  No combos found", flush=True)

        all_results.extend(results)

    # Save CSV per (thresh, period)
    if all_results:
        csv_name = f"backtest_v2_{tp_pct:.2f}_{max_hold}.csv"
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), csv_name)
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "thresh", "period", "combo_size", "conditions",
                "precision", "signals", "wins", "losses", "expired",
            ])
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\n  Saved to {csv_path}", flush=True)

    return all_results


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Tracker backtest (2-phase optimized)")
    parser.add_argument("--thresh", type=float, default=None)
    parser.add_argument("--period", type=int, default=None)
    parser.add_argument("--combo", type=int, default=None, help="Combo size (3,4,5). Default: 4,5")
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

    combo_sizes = [args.combo] if args.combo else [4, 5]

    print("=" * 70, flush=True)
    print("find_high_precision_v2 — 2-Phase Optimized Tracker Backtest", flush=True)
    print("=" * 70, flush=True)
    print(f"  Pairs         : {len(pairs)}", flush=True)
    print(f"  Combo sizes   : {combo_sizes}", flush=True)
    print(f"  Min signals   : {args.min_signals}", flush=True)
    print(f"  Min precision : {args.min_precision}%", flush=True)
    print(f"  SL = -20% | Same-day TP+SL = LOSS | Expired = failure", flush=True)
    print("=" * 70, flush=True)

    total_start = time.time()

    # Download
    ticker_data = download_all_data()
    if not ticker_data:
        print("ERROR: No data. Exiting.", flush=True)
        sys.exit(1)

    # Build arrays
    print("\n[3/4] Building combined arrays...", flush=True)
    cond_cols, close_arr, high_arr, low_arr, ticker_ranges = build_arrays(ticker_data)
    total_rows = len(close_arr)
    print(f"  {total_rows:,} rows across {len(ticker_ranges)} tickers", flush=True)
    del ticker_data

    # Warmup numba
    print("\n[4/4] Warming up Numba...", flush=True)
    _dummy = np.array([0], dtype=np.int64)
    _dc = np.array([10.0, 11.0, 12.0], dtype=np.float64)
    _eval_ticker_signals(_dummy, _dc, _dc, _dc, 3, 0.10, -0.20, 2)
    print("  Ready.", flush=True)

    # Run all pairs
    grand_results = []
    for tp_pct, max_hold in pairs:
        t0 = time.time()
        results = run_single(
            tp_pct, max_hold, combo_sizes,
            cond_cols, close_arr, high_arr, low_arr, ticker_ranges,
            args.min_signals, args.min_precision,
        )
        grand_results.extend(results)
        elapsed = time.time() - t0
        print(f"\n  >>> [{tp_pct:.2f}/{max_hold}d] {elapsed:.0f}s, {len(results)} results\n", flush=True)

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 70}", flush=True)
    print(f"DONE — {len(grand_results)} total combos in {total_elapsed/60:.1f} min", flush=True)
    print(f"{'=' * 70}", flush=True)

    if grand_results:
        grand_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_v2_all_results.csv")
        with open(grand_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "thresh", "period", "combo_size", "conditions",
                "precision", "signals", "wins", "losses", "expired",
            ])
            writer.writeheader()
            grand_results.sort(key=lambda r: (-r["signals"], -r["precision"]))
            writer.writerows(grand_results)
        print(f"  Saved to {grand_csv}", flush=True)

        print(f"\n  Top results:", flush=True)
        print(f"  {'Thresh':>6}  {'Period':>6}  {'Prec':>6}  {'Sigs':>5}  {'Win':>4}  {'Fail':>5}  Conditions", flush=True)
        for r in grand_results[:30]:
            print(f"  {r['thresh']:>6.2f}  {r['period']:>6}  {r['precision']:>5.1f}%  {r['signals']:>5}  {r['wins']:>4}  {r['losses']:>5}  {r['conditions']}", flush=True)

    print(f"\n{'=' * 70}", flush=True)


if __name__ == "__main__":
    main()
