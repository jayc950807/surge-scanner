#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Streamlit Dashboard
  Rolls-Royce Inspired Design — Dark Luxury Edition
================================================================================
"""
import streamlit as st
import pandas as pd
import json
import os
import glob
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# shared_config에서 공통 상수/함수 import (없으면 로컬 fallback)
try:
    from shared_config import KST, STRATEGY_CONFIG, STRATEGY_NAMES, STRATEGY_WINRATE, is_us_dst
except ImportError:
    KST = timezone(timedelta(hours=9))
    def is_us_dst(dt_date):
        from datetime import date as _d
        year = dt_date.year
        mar1 = _d(year, 3, 1)
        days_to_sun = (6 - mar1.weekday()) % 7
        dst_start = mar1 + timedelta(days=days_to_sun + 7)
        nov1 = _d(year, 11, 1)
        days_to_sun2 = (6 - nov1.weekday()) % 7
        dst_end = nov1 + timedelta(days=days_to_sun2)
        return dst_start <= dt_date < dst_end

# ─── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SURGE SCANNER",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Rolls-Royce Design System ──────────────────────────────────────────────
st.markdown("""
<style>
/* ════════════════════════════════════════════════════════════════
   DESIGN TOKENS — Rolls-Royce Inspired
   ════════════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@300;400;500;600;700&family=Space+Mono:wght@400;700&display=swap');

:root {
    --bg-deep:      #080b12;
    --bg-surface:   #0d1117;
    --bg-card:      #131920;
    --bg-elevated:  #1a2230;
    --bg-hover:     #1e2a3a;
    --border-subtle:#1c2636;
    --border-accent:#c9a96e22;
    --gold:         #c9a96e;
    --gold-dim:     #9a7d4e;
    --gold-glow:    #c9a96e18;
    --text-primary: #e8e4de;
    --text-secondary:#8a9ab5;
    --text-muted:   #4d5b72;
    --green:        #4a9e7d;
    --green-bright: #5cc49a;
    --red:          #9e4a5a;
    --red-bright:   #c46b7c;
    --blue:         #4a7a9e;
    --amber:        #b8954a;
    --font-display: 'Cormorant Garamond', Georgia, serif;
    --font-body:    'Plus Jakarta Sans', -apple-system, sans-serif;
    --font-mono:    'Space Mono', monospace;
}

/* ── Global Reset ── */
.main .block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 1400px;
}
.main { background: var(--bg-deep) !important; }
[data-testid="stAppViewContainer"] { background: var(--bg-deep) !important; }
[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stSidebar"] { background: var(--bg-surface) !important; }
section[data-testid="stSidebar"] .block-container { padding-top: 2rem; }

/* ── Streamlit element overrides ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    background: var(--bg-surface);
    border-radius: 8px;
    padding: 4px;
    border: 1px solid var(--border-subtle);
}
.stTabs [data-baseweb="tab"] {
    font-family: var(--font-body) !important;
    font-weight: 500 !important;
    font-size: 0.82em !important;
    letter-spacing: 0.03em;
    color: var(--text-muted) !important;
    padding: 8px 16px !important;
    border-radius: 6px !important;
    border: none !important;
    background: transparent !important;
    white-space: nowrap !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color: var(--gold) !important;
    background: var(--bg-elevated) !important;
    border-bottom: none !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }
.stTabs [data-baseweb="tab-border"] { display: none !important; }

/* Streamlit divider override */
hr { border-color: var(--border-subtle) !important; opacity: 0.4 !important; }

/* Selectbox/button styling */
[data-testid="stSelectbox"] label { color: var(--text-secondary) !important; font-size: 0.82em !important; }
.stSelectbox > div > div { background: var(--bg-card) !important; border-color: var(--border-subtle) !important; }
.stButton > button {
    background: var(--bg-elevated) !important;
    color: var(--gold) !important;
    border: 1px solid var(--gold-dim) !important;
    border-radius: 6px !important;
    font-family: var(--font-body) !important;
    font-weight: 500 !important;
    letter-spacing: 0.03em;
}
.stButton > button:hover { background: var(--bg-hover) !important; }

/* Expander styling */
[data-testid="stExpander"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: 8px !important;
}
[data-testid="stExpander"] summary { color: var(--text-primary) !important; font-family: var(--font-body) !important; }

/* Toggle/slider */
.stToggle label { color: var(--text-secondary) !important; }

/* ════════════════════════════════════════════════════════════════
   HEADER
   ════════════════════════════════════════════════════════════════ */
.rr-header {
    text-align: center;
    padding: 28px 0 20px 0;
}
.rr-header .brand {
    font-family: var(--font-display);
    font-size: 1.8em;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: 0.12em;
    margin-bottom: 6px;
}
.rr-header .brand span { color: var(--gold); }
.rr-header .sub {
    font-family: var(--font-body);
    font-size: 0.78em;
    color: var(--text-muted);
    letter-spacing: 0.15em;
    text-transform: uppercase;
}
.rr-divider {
    width: 60px;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--gold), transparent);
    margin: 16px auto;
}
.rr-meta {
    font-family: var(--font-mono);
    font-size: 0.72em;
    color: var(--text-muted);
    text-align: center;
    letter-spacing: 0.04em;
    margin-bottom: 24px;
}
.rr-meta span { margin: 0 12px; }
.rr-meta .hl { color: var(--text-secondary); }

/* ════════════════════════════════════════════════════════════════
   SUMMARY CARDS
   ════════════════════════════════════════════════════════════════ */
.rr-cards {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 12px;
    margin-bottom: 24px;
}
.rr-card {
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: 10px;
    padding: 20px 16px;
    text-align: center;
    position: relative;
    overflow: hidden;
}
.rr-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--border-subtle);
}
.rr-card.accent::before {
    background: linear-gradient(90deg, var(--gold-dim), var(--gold), var(--gold-dim));
}
.rr-card .val {
    font-family: var(--font-mono);
    font-size: 1.8em;
    font-weight: 700;
    line-height: 1.2;
    margin-bottom: 4px;
}
.rr-card .lbl {
    font-family: var(--font-body);
    font-size: 0.72em;
    color: var(--text-muted);
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

/* ════════════════════════════════════════════════════════════════
   MATRIX TABLE
   ════════════════════════════════════════════════════════════════ */
.rr-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-family: var(--font-body);
    font-size: 0.84em;
    background: var(--bg-card);
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid var(--border-subtle);
}
.rr-table th {
    background: var(--bg-elevated);
    color: var(--text-muted);
    padding: 12px 14px;
    text-align: center;
    font-weight: 500;
    font-size: 0.82em;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    border-bottom: 1px solid var(--border-subtle);
}
.rr-table th:first-child {
    text-align: left;
    padding-left: 20px;
}
.rr-table td {
    padding: 12px 14px;
    text-align: center;
    border-bottom: 1px solid #0d131a;
    color: var(--text-primary);
    font-variant-numeric: tabular-nums;
    font-family: var(--font-mono);
    font-size: 0.92em;
}
.rr-table td:first-child {
    text-align: left;
    padding-left: 20px;
    font-family: var(--font-body);
}
.rr-table tr:last-child td { border-bottom: none; }
.rr-table tr:hover td { background: var(--bg-hover); }
.rr-table tbody tr { transition: background 0.15s ease; }

/* ── Strategy Tags ── */
.stag {
    display: inline-block;
    padding: 3px 8px;
    border-radius: 4px;
    font-family: var(--font-mono);
    font-weight: 700;
    font-size: 0.74em;
    letter-spacing: 0.02em;
    border: 1px solid;
    white-space: nowrap;
}
.stag-1 { color: var(--green-bright); border-color: var(--green); background: #4a9e7d12; }
.stag-2 { color: var(--blue); border-color: #4a7a9e66; background: #4a7a9e0a; }
.stag-3 { color: var(--amber); border-color: #b8954a66; background: #b8954a0a; }
.stag-4 { color: var(--red-bright); border-color: var(--red); background: #9e4a5a0a; }
.stag-5 { color: #9a7abf; border-color: #7a5aaf66; background: #7a5aaf0a; }
.stag-6 { color: #e0a040; border-color: #c0882066; background: #c088200a; }
.stag-7 { color: #40b0a0; border-color: #309a8a66; background: #309a8a0a; }
.stag-8 { color: #d06070; border-color: #b0485866; background: #b048580a; }
.stag-9 { color: #70a0e0; border-color: #5088c066; background: #5088c00a; }
.stag-10 { color: #a080d0; border-color: #8868b866; background: #8868b80a; }
.stag-new { color: var(--gold); border-color: var(--gold-dim); background: #c9a96e12; }

/* ── Cell States ── */
.c-win { color: var(--green-bright); font-weight: 600; }
.c-loss { color: var(--red-bright); font-weight: 600; }
.c-partial { color: var(--amber); font-weight: 500; }
.c-pending { color: var(--text-muted); font-style: italic; font-weight: 400; }
.c-none { color: #2a3444; }
.c-muted { color: var(--text-muted); }

/* ── Status badges ── */
.st-win { color: var(--green-bright); font-weight: 600; }
.st-loss { color: var(--red-bright); font-weight: 600; }
.st-expired { color: var(--amber); font-weight: 600; }
.st-open { color: var(--blue); font-weight: 600; }
.st-pending { color: var(--text-muted); font-weight: 500; }

/* ── Progress bar ── */
.rr-prog {
    display: flex; align-items: center; gap: 6px;
}
.rr-prog-track {
    flex: 1; background: #1a2230; border-radius: 3px; height: 6px; min-width: 40px; overflow: hidden;
}
.rr-prog-fill { height: 100%; border-radius: 3px; transition: width 0.3s ease; }
.rr-prog-text { font-size: 0.78em; font-weight: 600; min-width: 32px; text-align: right; }

/* ── Ticker Group Header ── */
.tk-header {
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-bottom: none;
    border-radius: 10px 10px 0 0;
    padding: 14px 20px;
    margin-top: 16px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
}
.tk-header .tk-name {
    font-family: var(--font-display);
    font-weight: 700;
    font-size: 1.15em;
    color: var(--text-primary);
    letter-spacing: 0.04em;
}
.tk-header .tk-meta {
    font-family: var(--font-body);
    font-size: 0.82em;
    color: var(--text-muted);
    display: flex;
    align-items: center;
    gap: 14px;
}
.tk-header + .rr-table { border-radius: 0 0 10px 10px; border-top: none; }

/* ── Strategy Info Card (Strategy tabs) ── */
.rr-strat-info {
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: 10px;
    padding: 24px 28px;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.rr-strat-info::before {
    content: '';
    position: absolute;
    top: 0; left: 0; bottom: 0;
    width: 3px;
}
.rr-strat-info.si-1::before { background: var(--green); }
.rr-strat-info.si-2::before { background: var(--blue); }
.rr-strat-info.si-3::before { background: var(--amber); }
.rr-strat-info.si-4::before { background: var(--red); }
.rr-strat-info.si-5::before { background: #7a5aaf; }
.rr-strat-info.si-6::before { background: #c08820; }
.rr-strat-info.si-7::before { background: #309a8a; }
.rr-strat-info.si-8::before { background: #b04858; }
.rr-strat-info.si-9::before { background: #5088c0; }
.rr-strat-info.si-10::before { background: #8868b8; }
.rr-strat-info h3 {
    font-family: var(--font-display);
    font-size: 1.15em;
    color: var(--text-primary);
    margin: 0 0 12px 0;
    font-weight: 600;
}
.rr-strat-info .si-grid {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 16px;
    font-size: 0.85em;
}
.rr-strat-info .si-section { color: var(--text-secondary); line-height: 1.7; }
.rr-strat-info .si-label {
    color: var(--text-muted);
    font-size: 0.82em;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 6px;
    font-weight: 500;
}
.rr-strat-info .si-stat {
    display: inline-flex; align-items: baseline; gap: 6px;
}
.rr-strat-info .si-stat .big {
    font-family: var(--font-mono);
    font-size: 1.6em;
    font-weight: 700;
}

/* ── No Signal ── */
.rr-empty {
    text-align: center;
    padding: 48px 20px;
    color: var(--text-muted);
    font-family: var(--font-body);
    font-size: 0.9em;
    background: var(--bg-card);
    border-radius: 10px;
    border: 1px solid var(--border-subtle);
}

/* ── PnL stat row ── */
.rr-stats {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 20px;
}
.rr-stat {
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: 8px;
    padding: 16px 20px;
    flex: 1;
    min-width: 120px;
    text-align: center;
}
.rr-stat .s-label {
    font-family: var(--font-body);
    font-size: 0.72em;
    color: var(--text-muted);
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-bottom: 4px;
}
.rr-stat .s-value {
    font-family: var(--font-mono);
    font-size: 1.5em;
    font-weight: 700;
}

/* ── Legend ── */
.rr-legend {
    font-family: var(--font-body);
    font-size: 0.76em;
    color: var(--text-muted);
    margin: 8px 0 12px 0;
    letter-spacing: 0.02em;
}

/* ════════════════════════════════════════════════════════════════
   RESPONSIVE — MOBILE (<768px)
   ════════════════════════════════════════════════════════════════ */
@media (max-width: 768px) {
    .main .block-container { padding: 0.5rem 0.8rem; }
    .rr-header .brand { font-size: 1.3em; }
    .rr-header .sub { font-size: 0.68em; }
    .rr-meta { font-size: 0.65em; }
    .rr-meta span { margin: 0 6px; }

    /* Cards: 2 columns on mobile */
    .rr-cards {
        grid-template-columns: repeat(2, 1fr);
        gap: 8px;
    }
    .rr-card { padding: 14px 10px; }
    .rr-card .val { font-size: 1.4em; }
    .rr-card .lbl { font-size: 0.65em; }
    /* 5th card spans full width */
    .rr-cards .rr-card:nth-child(5) {
        grid-column: 1 / -1;
    }

    /* Tables: horizontal scroll */
    .rr-table-wrap {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        margin: 0 -0.5rem;
        padding: 0 0.5rem;
    }
    .rr-table {
        font-size: 0.76em;
        min-width: 600px;
    }
    .rr-table th, .rr-table td {
        padding: 10px 10px;
        white-space: nowrap;
    }

    /* Strategy info: single column */
    .rr-strat-info .si-grid {
        grid-template-columns: 1fr;
        gap: 12px;
    }
    .rr-strat-info { padding: 16px 18px; }

    /* Tabs: smaller text */
    .stTabs [data-baseweb="tab"] {
        font-size: 0.72em !important;
        padding: 6px 10px !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        flex-wrap: wrap;
    }

    /* Ticker header: stack */
    .tk-header {
        flex-direction: column;
        align-items: flex-start;
        padding: 12px 14px;
    }

    /* PnL stats: 2-col */
    .rr-stats {
        flex-wrap: wrap;
    }
    .rr-stat {
        min-width: calc(50% - 8px);
        flex: unset;
        padding: 12px 14px;
    }
    .rr-stat .s-value { font-size: 1.2em; }
}

/* ════════════════════════════════════════════════════════════════
   RESPONSIVE — SMALL MOBILE (<480px)
   ════════════════════════════════════════════════════════════════ */
@media (max-width: 480px) {
    .rr-header .brand { font-size: 1.1em; letter-spacing: 0.08em; }
    .rr-cards { grid-template-columns: 1fr 1fr; gap: 6px; }
    .rr-card .val { font-size: 1.2em; }
    .rr-table { min-width: 500px; font-size: 0.72em; }
    .stTabs [data-baseweb="tab"] { font-size: 0.65em !important; padding: 5px 8px !important; }
}
</style>
""", unsafe_allow_html=True)

# ─── Data Loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_latest_scan():
    p = "data/latest_scan.json"
    if os.path.exists(p):
        with open(p, 'r') as f: return json.load(f)
    return None

@st.cache_data(ttl=300)
def load_today_signals():
    """가장 최근 시그널 파일 로드 (날짜 하드코딩 대신 glob 최신 파일)"""
    files = sorted(glob.glob("data/signal_*.csv"), reverse=True)
    if files:
        df = pd.read_csv(files[0])
        return df if len(df) > 0 else pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=300)
def load_history():
    p = "data/history.csv"
    if os.path.exists(p):
        df = pd.read_csv(p)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()

@st.cache_data(ttl=300)
def load_tracker_summary():
    p = "data/tracker_summary.json"
    if os.path.exists(p):
        with open(p, 'r') as f: return json.load(f)
    return None

@st.cache_data(ttl=300)
def load_open_positions():
    p = "data/open_positions.csv"
    if os.path.exists(p):
        df = pd.read_csv(p, dtype=str)
        return df if len(df) > 0 else pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=300)
def load_closed_positions():
    p = "data/closed_positions.csv"
    if os.path.exists(p):
        df = pd.read_csv(p, dtype=str)
        return df if len(df) > 0 else pd.DataFrame()
    return pd.DataFrame()

# ─── Constants ────────────────────────────────────────────────────────────────
STRAT_TAB = {'1': '1차_1_+5%5일', '2': '1차_2_+15%10일', '3': '1차_3_+5%5일', '4': '1차_4_+20%30일', '5': '1차_5_+10%30일', '6': '2차_1_+50%20일', '7': '2차_2_+40%20일', '8': '2차_3_+40%20일', '9': '2차_4_+10%5일', '10': '2차_5_+10%5일'}
STRAT_NAMES = {'1': '1차_1_+5%5일 · 급락반등', '2': '1차_2_+15%10일 · 고수익', '3': '1차_3_+5%5일 · 과매도', '4': '1차_4_+20%30일 · 초저가', '5': '1차_5_+10%30일 · 속반등', '6': '2차_1_+50%20일 · 바닥급등', '7': '2차_2_+40%20일 · MACD전환', '8': '2차_3_+40%20일 · ATR확대', '9': '2차_4_+10%5일 · 과매도단타', '10': '2차_5_+10%5일 · MACD단타'}
STRAT_KR = {'1': '급락반등', '2': '고수익', '3': '과매도', '4': '초저가', '5': '속반등', '6': '바닥급등', '7': 'MACD전환', '8': 'ATR확대', '9': '과매도단타', '10': 'MACD단타'}
STRAT_TP = {'1': '+5%', '2': '+15%', '3': '+5%', '4': '+20%', '5': '+10%', '6': '+50%', '7': '+40%', '8': '+40%', '9': '+10%', '10': '+10%'}
STRAT_TP_NUM = {'1': 5, '2': 15, '3': 5, '4': 20, '5': 10, '6': 50, '7': 40, '8': 40, '9': 10, '10': 10}
STRAT_BT_WR = {'1': '90.1%', '2': '90.3%', '3': '86.9%', '4': '97.7%', '5': '91.0%', '6': '87.5%', '7': '90.0%', '8': '90.0%', '9': '100.0%', '10': '90.0%'}
STRAT_MAX_HOLD = {'1': 5, '2': 10, '3': 5, '4': 30, '5': 30, '6': 20, '7': 20, '8': 20, '9': 5, '10': 5}
# 탭 표시 순서: TP% 오름차순, 동일 TP%일 때 보유일 오름차순
STRAT_ORDER = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

# Populate strategy dicts for new strategies (11-306) from STRATEGY_CONFIG
try:
    _new_keys = sorted([k for k in STRATEGY_CONFIG.keys() if k not in STRAT_KR], key=lambda x: int(x) if x.isdigit() else 999)
    for _i, _k in enumerate(_new_keys, start=1):
        _cfg = STRATEGY_CONFIG[_k]
        _tp = _cfg.get('tp_pct', 0)
        _mh = _cfg.get('max_hold', 5)
        STRAT_KR[_k] = f'3차_{_i}'
        STRAT_TP[_k] = f'+{int(_tp*100)}%'
        STRAT_TP_NUM[_k] = int(_tp * 100)
        STRAT_MAX_HOLD[_k] = _mh
        STRAT_TAB[_k] = f'3차_{_i}_+{int(_tp*100)}%{_mh}일'
        STRAT_NAMES[_k] = f'3차_{_i}_+{int(_tp*100)}%{_mh}일'
except NameError:
    pass

# Populate STRAT_BT_WR for new strategies from shared_config
try:
    for _k, _v in STRATEGY_WINRATE.items():
        if _k not in STRAT_BT_WR:
            STRAT_BT_WR[_k] = _v
except NameError:
    pass

# ─── Helpers ──────────────────────────────────────────────────────────────────
def safe_str(val, fb='—'):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN'): return fb
    return str(val)

def safe_float(val, fb=0):
    try:
        v = float(val)
        return v if not pd.isna(v) else fb
    except: return fb

def stag(s):
    cls = f'stag-{s}' if s in ('1','2','3','4','5','6','7','8','9','10') else 'stag-new'
    return f'<span class="stag {cls}">{STRAT_TAB.get(s, s)}</span>'

def cell_html(ach, det, loss=0, prog=0):
    """Render matrix cell: ach=wins, det=total, loss=loss+expired, prog=open+pending"""
    if det == 0: return '<span class="c-none">—</span>'
    if ach == det: return f'<span class="c-win">{ach}/{det}</span>'
    # All still in progress (no closed results yet)
    if prog > 0 and loss == 0 and ach == 0 and prog == det:
        return f'<span class="c-pending">{det}</span>'
    # Build composite: wins / losses / in-progress
    parts = []
    closed = ach + loss
    if ach > 0: parts.append(f'<span class="c-win">{ach}</span>')
    if loss > 0: parts.append(f'<span class="c-loss">{loss}</span>')
    if prog > 0: parts.append(f'<span class="c-pending">{prog}</span>')
    if not parts:
        return f'<span class="c-partial">{ach}/{det}</span>'
    # Format: green·red·gray / total
    sep = '<span class="c-muted">·</span>'
    color = 'c-win' if ach > 0 and loss == 0 else 'c-loss' if ach == 0 and loss > 0 else 'c-partial'
    return f'{sep.join(parts)} <span class="c-muted">/</span> <span class="{color}">{det}</span>'

def progress_bar(val, color):
    w = min(val, 100)
    return f'''<div class="rr-prog">
        <div class="rr-prog-track"><div class="rr-prog-fill" style="width:{w:.0f}%;background:{color}"></div></div>
        <span class="rr-prog-text" style="color:{color}">{val:.0f}%</span>
    </div>'''

def chg_html(val_raw):
    v = safe_float(val_raw)
    if val_raw == '—': return '<span class="c-muted">—</span>'
    cls = 'c-win' if v > 0 else 'c-loss' if v < 0 else 'c-none'
    return f'<span class="{cls}">{v:+.1f}%</span>'

def result_badge(r):
    # v4: TRAILING 상태는 더 이상 생성되지 않으므로 매핑에서 제거.
    m = {'WIN': ('st-win', 'WIN'), 'LOSS': ('st-loss', 'LOSS'), 'EXPIRED': ('st-expired', 'EXP'),
         'OPEN': ('st-open', 'OPEN'), 'PENDING': ('st-pending', 'WAIT')}
    cls, txt = m.get(r, ('c-muted', r))
    return f'<span class="{cls}">{txt}</span>'

def us_to_kst(us_date_str):
    """미국 거래일(YYYY-MM-DD) → 한국 날짜/시간 변환.
    미국 장마감 4:00 PM ET 기준으로 KST 변환.
    EDT(서머타임): +13h → 익일 05:00 KST
    EST(겨울):     +14h → 익일 06:00 KST
    """
    if not us_date_str or us_date_str in ('—', 'nan', 'None', ''):
        return '—'
    try:
        from datetime import date as _d
        dt = datetime.strptime(str(us_date_str)[:10], '%Y-%m-%d')
        d = dt.date() if hasattr(dt, 'date') else dt
        if is_us_dst(d):
            kst_dt = dt + timedelta(hours=13)  # EDT +13h
        else:
            kst_dt = dt + timedelta(hours=14)  # EST +14h
        return kst_dt.strftime('%m/%d %H:%M KST')
    except Exception:
        return str(us_date_str)

def us_to_kst_date(us_date_str):
    """미국 거래일 → 한국 날짜만 (YYYY-MM-DD)"""
    if not us_date_str or us_date_str in ('—', 'nan', 'None', ''):
        return '—'
    try:
        from datetime import date as _d
        dt = datetime.strptime(str(us_date_str)[:10], '%Y-%m-%d')
        d = dt.date() if hasattr(dt, 'date') else dt
        if is_us_dst(d):
            kst_dt = dt + timedelta(hours=13)
        else:
            kst_dt = dt + timedelta(hours=14)
        return kst_dt.strftime('%Y-%m-%d')
    except Exception:
        return str(us_date_str)

def us_to_kst_short(us_date_str):
    """미국 거래일 → 한국 날짜 짧은형 (MM/DD)"""
    if not us_date_str or us_date_str in ('—', 'nan', 'None', ''):
        return '—'
    try:
        from datetime import date as _d
        dt = datetime.strptime(str(us_date_str)[:10], '%Y-%m-%d')
        d = dt.date() if hasattr(dt, 'date') else dt
        if is_us_dst(d):
            kst_dt = dt + timedelta(hours=13)
        else:
            kst_dt = dt + timedelta(hours=14)
        return kst_dt.strftime('%m/%d')
    except Exception:
        return str(us_date_str)

def calc_sell_date_kst(us_date_str, hold_days):
    """미국 탐지일 + hold_days 영업일 후의 KST 날짜 계산 (매도 예정일)"""
    if not us_date_str or us_date_str in ('—', 'nan', 'None', ''):
        return '—'
    try:
        dt = pd.to_datetime(us_date_str)
        sell_dt_us = dt + pd.offsets.BDay(hold_days)
        return us_to_kst_short(sell_dt_us.strftime('%Y-%m-%d'))
    except Exception:
        return '—'

# ─── Header ───────────────────────────────────────────────────────────────────
scan_info = load_latest_scan()
tracker_info = load_tracker_summary()

st.markdown('''<div class="rr-header">
    <div class="brand">SURGE <span>SCANNER</span></div>
    <div class="sub">US Equity Mean-Reversion Intelligence</div>
</div>
<div class="rr-divider"></div>''', unsafe_allow_html=True)

meta_parts = []
if scan_info:
    meta_parts.append(f'<span class="hl">Last Scan</span> {scan_info.get("scan_time", "N/A")}')
    counts = ' / '.join(f'{k}:{scan_info.get(f"strategy_{k}_count", 0)}' for k in ['1','2','3','4','5','6','7','8','9','10'])
    meta_parts.append(f'<span class="hl">Signals</span> {counts}')
if tracker_info:
    meta_parts.append(f'<span class="hl">Active</span> {tracker_info.get("open_count", 0)} <span class="hl">Win</span> {tracker_info.get("win_count", 0)} <span class="hl">Loss</span> {tracker_info.get("loss_count", 0)}')
if meta_parts:
    st.markdown(f'<div class="rr-meta">{"<span>|</span>".join(meta_parts)}</div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="rr-meta">Awaiting first scan...</div>', unsafe_allow_html=True)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
today_signals = load_today_signals()
history = load_history()

# Count today's signals for tab label
_today_n = len(today_signals) if not today_signals.empty else 0
_today_label = f"Today ({_today_n})" if _today_n > 0 else "Today"

tab_today, tab_1, tab_3, tab_9, tab_10, tab_5, tab_2, tab_7, tab_8, tab_6, tab_4, tab_new, tab_history = st.tabs([
    _today_label, "1_5%5일", "3_5%5일", "9_10%5일", "10_10%5일", "5_10%30일", "2_15%10일", "7_40%20일", "8_40%20일", "6_50%20일", "4_20%30일", "New (11+)", "Performance"
])

# ─── Strategy Tab Builder ─────────────────────────────────────────────────────
STRAT_INFO = {
    '1': {
        'title': 'A_5%5일 — 급락반등',
        'desc': ['RSI(7) < 20 — 극단적 과매도', '일중 변동 > 20% — 패닉셀링', '3일 수익률 < -15%', '연속 하락 > 5일', '5일 저점 대비 5% 이내'],
        'rules': ['매수: 신호 당일 종가 (애프터마켓)', '익절: +5%', '손절: -20%', '트레일링: -3%', '최대 보유: 5일'],
        'bt': '90.1% (236/262)',
        'cols': ['ticker','date','scan_time','price','rsi7','intraday','ret3d','consec_down','dist_low5','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi7": st.column_config.NumberColumn("RSI 7", format="%.1f"),
            "intraday": st.column_config.NumberColumn("Intra%", format="%.1f%%"),
            "ret3d": st.column_config.NumberColumn("Ret 3D", format="%.1f%%"),
            "consec_down": st.column_config.NumberColumn("Consec", format="%d"),
            "dist_low5": st.column_config.NumberColumn("Dist L5", format="%.1f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '2': {
        'title': 'D_15%10일 — 고수익',
        'desc': ['RSI(7) < 20 + RSI(14) < 35', 'ATR 비율 > 3 — 변동성 폭발', '일중 변동 > 15%', '20일 이평 대비 -25% 이하', '매출 성장률 > 0 — 펀더멘탈 필터'],
        'rules': ['매수: 신호 당일 종가 (애프터마켓)', '익절: +15%', '손절: -20%', '최대 보유: 10일'],
        'bt': '90.3% (28/31)',
        'cols': ['ticker','date','scan_time','price','rsi7','rsi14','atr_ratio','intra_pct','ma20_pos','rev_growth','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi7": st.column_config.NumberColumn("RSI 7", format="%.1f"),
            "rsi14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
            "atr_ratio": st.column_config.NumberColumn("ATR", format="%.2f"),
            "intra_pct": st.column_config.NumberColumn("Intra%", format="%.1f%%"),
            "ma20_pos": st.column_config.NumberColumn("MA20%", format="%.1f%%"),
            "rev_growth": st.column_config.NumberColumn("Rev Gr", format="%.1f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '3': {
        'title': 'B_5%5일 — 과매도',
        'desc': ['RSI(7) < 30 — 과매도', '일중 변동 > 20%', '당일 수익률 < -8%', '전일도 하락 (2일 연속)', '연속 하락 > 3일', '5일 저점 대비 3% 이내'],
        'rules': ['매수: 신호 당일 종가 (애프터마켓)', '익절: +5%', '손절: -20%', '최대 보유: 5일'],
        'bt': '86.9% (542/624)',
        'cols': ['ticker','date','scan_time','price','rsi7','intraday','ret1d','consec_down','dist_low5','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi7": st.column_config.NumberColumn("RSI 7", format="%.1f"),
            "intraday": st.column_config.NumberColumn("Intra%", format="%.1f%%"),
            "ret1d": st.column_config.NumberColumn("Ret 1D", format="%.1f%%"),
            "consec_down": st.column_config.NumberColumn("Consec", format="%d"),
            "dist_low5": st.column_config.NumberColumn("Dist L5", format="%.2f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '4': {
        'title': 'E_20%30일 — 초저가',
        'desc': ['종가 ≤ $3 — 초저가주', '5일 수익률 ≤ -40%', '일중 변동 ≥ 30%', 'RSI(14) ≤ 25'],
        'rules': ['매수: 신호 당일 종가 (애프터마켓)', '익절: +20% (중간값 2일 도달)', '손절: 없음', '최대 보유: 30일'],
        'bt': '97.7% (127/130)',
        'cols': ['ticker','date','scan_time','price','rsi14','intraday','ret5d','tp_price','hold_days'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
            "intraday": st.column_config.NumberColumn("Intra%", format="%.1f%%"),
            "ret5d": st.column_config.NumberColumn("Ret 5D", format="%.1f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "hold_days": st.column_config.NumberColumn("Hold", format="%d"),
        },
    },
    '5': {
        'title': 'C_10%30일 — 속반등',
        'desc': ['종가 $3~$10', '5일 수익률 ≤ -25%', '일중 변동 ≥ 20%', '연속 하락 ≥ 5일', '평균 거래량 ≥ 200K'],
        'rules': ['매수: 신호 당일 종가 (애프터마켓)', '익절: +10% (중간값 2일 도달)', '손절: 없음', '최대 보유: 30일'],
        'bt': '91.0% (273/300)',
        'cols': ['ticker','date','scan_time','price','ret5d','intraday','consec_down','vol_avg','tp_price','hold_days'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "ret5d": st.column_config.NumberColumn("Ret 5D", format="%.1f%%"),
            "intraday": st.column_config.NumberColumn("Intra%", format="%.1f%%"),
            "consec_down": st.column_config.NumberColumn("Consec", format="%d"),
            "vol_avg": st.column_config.NumberColumn("Avg Vol", format="%d"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "hold_days": st.column_config.NumberColumn("Hold", format="%d"),
        },
    },
    '6': {
        'title': 'F_50%20일 — 바닥급등',
        'desc': ['20일 변동성 > 10%', '52주 고점 대비 -85% 이하', '볼린저 %B < 0', '전일 수익률 < -5%', '갭업 > 5%'],
        'rules': ['매수: 신호 당일 종가', '익절: +50%', '손절: -20%', '최대 보유: 20일'],
        'bt': '87.5% (7/8)',
        'cols': ['ticker','date','scan_time','price','vol_20d','dist_52w','bb_pctb','ret_1d','gap_pct','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "vol_20d": st.column_config.NumberColumn("Vol 20D", format="%.1f%%"),
            "dist_52w": st.column_config.NumberColumn("52W Dist", format="%.1f%%"),
            "bb_pctb": st.column_config.NumberColumn("BB %B", format="%.3f"),
            "ret_1d": st.column_config.NumberColumn("Ret 1D", format="%.1f%%"),
            "gap_pct": st.column_config.NumberColumn("Gap", format="%.1f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '7': {
        'title': 'G_40%20일 — MACD전환',
        'desc': ['20일 변동성 > 10%', 'RSI(7) < 30', 'MACD 히스토그램 골든크로스', '갭업 > 5%', '종가 < SMA(5)'],
        'rules': ['매수: 신호 당일 종가', '익절: +40%', '손절: -20%', '최대 보유: 20일'],
        'bt': '90.0% (9/10)',
        'cols': ['ticker','date','scan_time','price','rsi7','vol_20d','macd_hist','gap_pct','sma5','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi7": st.column_config.NumberColumn("RSI 7", format="%.1f"),
            "vol_20d": st.column_config.NumberColumn("Vol 20D", format="%.1f%%"),
            "macd_hist": st.column_config.NumberColumn("MACD H", format="%.4f"),
            "gap_pct": st.column_config.NumberColumn("Gap", format="%.1f%%"),
            "sma5": st.column_config.NumberColumn("SMA5", format="$%.2f"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '8': {
        'title': 'H_40%20일 — ATR확대',
        'desc': ['RSI(7) < 30', '52주 고점 대비 -85% 이하', '전일 수익률 < -5%', '갭업 > 5%', 'ATR 5일 변화 > 25%'],
        'rules': ['매수: 신호 당일 종가', '익절: +40%', '손절: -20%', '최대 보유: 20일'],
        'bt': '90.0% (9/10)',
        'cols': ['ticker','date','scan_time','price','rsi7','dist_52w','ret_1d','gap_pct','atr_change','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi7": st.column_config.NumberColumn("RSI 7", format="%.1f"),
            "dist_52w": st.column_config.NumberColumn("52W Dist", format="%.1f%%"),
            "ret_1d": st.column_config.NumberColumn("Ret 1D", format="%.1f%%"),
            "gap_pct": st.column_config.NumberColumn("Gap", format="%.1f%%"),
            "atr_change": st.column_config.NumberColumn("ATR Chg", format="%.1f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '9': {
        'title': 'I_10%5일 — 과매도단타',
        'desc': ['RSI(14) < 30', '거래량 2배 이상', 'MACD 히스토그램 > 0', '전일 수익률 < -5%', '5일 수익률 > 0'],
        'rules': ['매수: 신호 당일 종가', '익절: +10%', '손절: -20%', '최대 보유: 5일'],
        'bt': '100.0% (8/8)',
        'cols': ['ticker','date','scan_time','price','rsi14','vol_ratio','macd_hist','ret_1d','ret_5d','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "rsi14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
            "vol_ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2f"),
            "macd_hist": st.column_config.NumberColumn("MACD H", format="%.4f"),
            "ret_1d": st.column_config.NumberColumn("Ret 1D", format="%.1f%%"),
            "ret_5d": st.column_config.NumberColumn("Ret 5D", format="%.1f%%"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
    '10': {
        'title': 'J_10%5일 — MACD단타',
        'desc': ['20일 변동성 > 10%', 'RSI(14) < 30', '거래량 2배 이상', 'MACD 골든크로스', '스토캐스틱 K < 20'],
        'rules': ['매수: 신호 당일 종가', '익절: +10%', '손절: -20%', '최대 보유: 5일'],
        'bt': '90.0% (9/10)',
        'cols': ['ticker','date','scan_time','price','vol_20d','rsi14','vol_ratio','macd_hist','stoch_k','tp_price','sl_price'],
        'col_config': {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "date": st.column_config.TextColumn("Date"),
            "scan_time": st.column_config.TextColumn("Scan"),
            "price": st.column_config.NumberColumn("Close", format="$%.2f"),
            "vol_20d": st.column_config.NumberColumn("Vol 20D", format="%.1f%%"),
            "rsi14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
            "vol_ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2f"),
            "macd_hist": st.column_config.NumberColumn("MACD H", format="%.4f"),
            "stoch_k": st.column_config.NumberColumn("Stoch K", format="%.1f"),
            "tp_price": st.column_config.NumberColumn("TP", format="$%.2f"),
            "sl_price": st.column_config.NumberColumn("SL", format="$%.2f"),
        },
    },
}

def render_strategy_tab(key, tab_obj):
    info = STRAT_INFO[key]
    with tab_obj:
        # Strategy info card
        desc_html = ''.join(f'<div>{d}</div>' for d in info['desc'])
        rules_html = ''.join(f'<div>{r}</div>' for r in info['rules'])
        st.markdown(f'''<div class="rr-strat-info si-{key}">
            <h3>{stag(key)} {info["title"]}</h3>
            <div class="si-grid">
                <div class="si-section"><div class="si-label">Entry Conditions</div>{desc_html}</div>
                <div class="si-section"><div class="si-label">Exit Rules</div>{rules_html}</div>
                <div class="si-section"><div class="si-label">Backtest</div>
                    <div class="si-stat"><span class="big" style="color:var(--gold)">{info["bt"].split("(")[0].strip()}</span></div>
                    <div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">({info["bt"].split("(")[1] if "(" in info["bt"] else ""})</div>
                </div>
            </div>
        </div>''', unsafe_allow_html=True)

        # Today's signals
        if not today_signals.empty and 'strategy' in today_signals.columns:
            sig = today_signals[today_signals['strategy'] == key]
        else:
            sig = pd.DataFrame()

        if not sig.empty:
            st.markdown(f'<div class="rr-legend" style="color:var(--gold)">Today — {len(sig)} signal{"s" if len(sig)>1 else ""} detected</div>', unsafe_allow_html=True)
            avail = [c for c in info['cols'] if c in sig.columns]
            st.dataframe(sig[avail].reset_index(drop=True), use_container_width=True, column_config=info['col_config'])
        else:
            st.markdown(f'<div class="rr-empty">No {STRAT_TAB.get(key, key)} signals today</div>', unsafe_allow_html=True)

        # Recent 30d history
        if not history.empty and 'strategy' in history.columns:
            hist = history[history['strategy'] == key]
            if not hist.empty:
                cutoff = datetime.now(KST).replace(tzinfo=None) - timedelta(days=30)
                recent = hist[hist['date'] >= cutoff]
                if not recent.empty:
                    st.markdown('<div class="rr-divider" style="margin:20px auto"></div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="rr-legend">Recent 30 Days — {len(recent)} signals</div>', unsafe_allow_html=True)
                    show = recent.sort_values('date', ascending=False).reset_index(drop=True)
                    show['date'] = show['date'].dt.strftime('%Y-%m-%d')
                    avail = [c for c in info['cols'] if c in show.columns]
                    st.dataframe(show[avail], use_container_width=True, column_config=info['col_config'])

# ─── Tab: Today's Picks ──────────────────────────────────────────────────────
# Pre-load closed positions for today's sell section
_closed_for_today = load_closed_positions()

# ── 전략별 실제 승률 계산 (현재까지 누적) ──
_actual_wr = {}
if not _closed_for_today.empty and 'strategy' in _closed_for_today.columns:
    _wr_rs = 'result_status' if 'result_status' in _closed_for_today.columns else 'status'
    for _sk in _closed_for_today['strategy'].unique():
        _sk_rows = _closed_for_today[_closed_for_today['strategy'] == _sk]
        _sk_upper = _sk_rows[_wr_rs].astype(str).str.upper()
        _sk_win = int((_sk_upper == 'WIN').sum())
        _sk_fail = int((_sk_upper.isin(['LOSS', 'EXPIRED'])).sum())
        _sk_total = _sk_win + _sk_fail
        if _sk_total > 0:
            _actual_wr[_sk] = f'{_sk_win}/{_sk_total} ({_sk_win/_sk_total*100:.0f}%)'
        else:
            _actual_wr[_sk] = '—'

_today_sells = pd.DataFrame()
if not _closed_for_today.empty and 'close_date' in _closed_for_today.columns:
    # close_date(미국 날짜)를 KST로 변환 후 오늘(KST)과 비교
    _closed_for_today['_cd_kst'] = _closed_for_today['close_date'].apply(us_to_kst_date)
    _today_dt = datetime.now(KST).strftime('%Y-%m-%d')
    _today_sells = _closed_for_today[_closed_for_today['_cd_kst'] == _today_dt]

with tab_today:
    # ── Today's Sells (매도) ──
    if not _today_sells.empty:
        _sell_rs = 'result_status' if 'result_status' in _today_sells.columns else 'status'
        st.markdown(f'''<div class="rr-legend" style="color:var(--red-bright);font-size:0.9em;margin-bottom:16px">
            SELL Today — <span style="font-weight:700;font-size:1.1em">{len(_today_sells)}</span> 종목 매도
        </div>''', unsafe_allow_html=True)
        sell_html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
        sell_html += '<th>Strategy</th><th>Ticker</th><th>Entry</th><th>Close $</th>'
        sell_html += '<th>P&L</th><th>Result</th><th>Days</th><th>Peak</th>'
        sell_html += '</tr></thead><tbody>'
        for _, sr in _today_sells.iterrows():
            s_ = safe_str(sr.get('strategy'))
            tk_ = safe_str(sr.get('ticker'))
            ep_ = safe_str(sr.get('entry_price'))
            cp_ = safe_str(sr.get('close_price'))
            rp_ = safe_str(sr.get('result_pct'))
            res_ = safe_str(sr.get(_sell_rs))
            dh_ = safe_str(sr.get('days_held'))
            mx_ = safe_str(sr.get('max_price'))
            ep_h = f'${ep_}' if ep_ != '—' else '—'
            cp_h = f'${cp_}' if cp_ != '—' else '—'
            rp_v = safe_float(rp_)
            rp_cls = 'c-win' if rp_v > 0 else 'c-loss' if rp_v < 0 else 'c-muted'
            rp_h = f'<span class="{rp_cls}">{rp_v:+.1f}%</span>' if rp_ != '—' else '—'
            res_h = result_badge(res_)
            mx_h = f'${mx_}' if mx_ != '—' else '—'
            sell_html += f'<tr>'
            sell_html += f'<td>{stag(s_)}</td>'
            sell_html += f'<td style="font-weight:700;color:var(--text-primary);font-size:1.05em">{tk_}</td>'
            sell_html += f'<td>{ep_h}</td><td>{cp_h}</td>'
            sell_html += f'<td>{rp_h}</td><td>{res_h}</td>'
            sell_html += f'<td>{dh_}일</td><td>{mx_h}</td>'
            sell_html += f'</tr>'
        sell_html += '</tbody></table></div>'
        st.markdown(sell_html, unsafe_allow_html=True)
        st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

    # ── Today's Buys (매수) ──
    if today_signals.empty:
        if _today_sells.empty:
            st.markdown('<div class="rr-empty">오늘 감지된 신호가 없습니다</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'''<div class="rr-legend" style="color:var(--green-bright);font-size:0.9em;margin-bottom:16px">
            BUY Today — <span style="font-weight:700;font-size:1.1em">{len(today_signals)}</span> 종목 매수
        </div>''', unsafe_allow_html=True)

        # Build the today table grouped by strategy
        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
        html += '<th>Strategy</th><th>Target</th><th>Ticker</th>'
        html += '<th>Price</th><th>TP Price</th><th>매도기한</th><th>실제승률</th><th>BT승률</th>'
        html += '</tr></thead><tbody>'

        for s_key in STRAT_ORDER:
            if 'strategy' not in today_signals.columns:
                continue
            s_sig = today_signals[today_signals['strategy'] == s_key]
            if s_sig.empty:
                continue
            for idx, row in s_sig.iterrows():
                tk = safe_str(row.get('ticker'))
                pr = safe_float(row.get('price', 0))
                tp = safe_float(row.get('tp_price', 0))
                sig_date = safe_str(row.get('date'))
                mh = STRAT_MAX_HOLD.get(s_key, 5)
                pr_h = f'${pr:.2f}' if pr > 0 else '—'
                tp_h = f'<span style="color:var(--gold);font-weight:600">${tp:.2f}</span>' if tp > 0 else '—'
                sell_by = calc_sell_date_kst(sig_date, mh)

                _awr = _actual_wr.get(s_key, '—')
                html += f'<tr>'
                html += f'<td>{stag(s_key)} <span style="color:var(--text-muted);font-size:0.85em">{STRAT_KR.get(s_key,"")}</span></td>'
                html += f'<td style="color:var(--gold);font-weight:700;font-size:1.05em">{STRAT_TP.get(s_key,"")}</td>'
                html += f'<td style="font-weight:700;color:var(--text-primary);font-size:1.05em">{tk}</td>'
                html += f'<td>{pr_h}</td>'
                html += f'<td>{tp_h}</td>'
                html += f'<td style="color:var(--red-bright);font-weight:600">{sell_by} <span style="font-size:0.8em;color:var(--text-muted)">({mh}일)</span></td>'
                html += f'<td style="color:var(--green-bright);font-weight:600">{_awr}</td>'
                html += f'<td style="color:var(--text-muted)">{STRAT_BT_WR.get(s_key,"—")}</td>'
                html += f'</tr>'

        # New strategies (11+)
        if 'strategy' in today_signals.columns:
            new_keys = sorted([k for k in today_signals['strategy'].unique() if k not in STRAT_ORDER], key=lambda x: int(x) if x.isdigit() else 999)
            for s_key in new_keys:
                s_sig = today_signals[today_signals['strategy'] == s_key]
                for idx, row in s_sig.iterrows():
                    tk = safe_str(row.get('ticker'))
                    pr = safe_float(row.get('price', 0))
                    tp = safe_float(row.get('tp_price', 0))
                    sig_date = safe_str(row.get('date'))
                    mh = STRAT_MAX_HOLD.get(s_key, 5)
                    pr_h = f'${pr:.2f}' if pr > 0 else '—'
                    tp_h = f'<span style="color:var(--gold);font-weight:600">${tp:.2f}</span>' if tp > 0 else '—'
                    sell_by = calc_sell_date_kst(sig_date, mh)
                    _awr = _actual_wr.get(s_key, '—')
                    html += f'<tr>'
                    html += f'<td>{stag(s_key)} <span style="color:var(--text-muted);font-size:0.85em">{STRAT_KR.get(s_key,"")}</span></td>'
                    html += f'<td style="color:var(--gold);font-weight:700;font-size:1.05em">{STRAT_TP.get(s_key,"")}</td>'
                    html += f'<td style="font-weight:700;color:var(--text-primary);font-size:1.05em">{tk}</td>'
                    html += f'<td>{pr_h}</td>'
                    html += f'<td>{tp_h}</td>'
                    html += f'<td style="color:var(--red-bright);font-weight:600">{sell_by} <span style="font-size:0.8em;color:var(--text-muted)">({mh}일)</span></td>'
                    html += f'<td style="color:var(--green-bright);font-weight:600">{_awr}</td>'
                    html += f'<td style="color:var(--text-muted)">{STRAT_BT_WR.get(s_key,"—")}</td>'
                    html += f'</tr>'

        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)

        # Strategy breakdown summary
        st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)
        strat_counts = today_signals.groupby('strategy').size() if 'strategy' in today_signals.columns else pd.Series(dtype=int)
        cards_html = '<div class="rr-cards" style="grid-template-columns:repeat(auto-fit,minmax(120px,1fr))">'
        for s_key in STRAT_ORDER:
            cnt = int(strat_counts.get(s_key, 0))
            if cnt == 0:
                continue
            cards_html += f'''<div class="rr-card">
                <div class="val" style="color:var(--text-primary)">{cnt}</div>
                <div class="lbl">{STRAT_TAB.get(s_key, s_key)}</div>
                <div style="font-size:0.68em;color:var(--text-muted);margin-top:4px">{STRAT_TP.get(s_key,"")} / {STRAT_MAX_HOLD.get(s_key,"")}일</div>
            </div>'''
        cards_html += '</div>'
        st.markdown(cards_html, unsafe_allow_html=True)

for k, t in [('1', tab_1), ('3', tab_3), ('9', tab_9), ('10', tab_10), ('5', tab_5), ('2', tab_2), ('7', tab_7), ('8', tab_8), ('6', tab_6), ('4', tab_4)]:
    render_strategy_tab(k, t)

with tab_new:
    st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">New Strategies (11~306) — 백테스트 90%+ 적중률 전략</div>', unsafe_allow_html=True)
    if not today_signals.empty and 'strategy' in today_signals.columns:
        new_sig = today_signals[~today_signals['strategy'].isin(['1','2','3','4','5','6','7','8','9','10'])]
        if not new_sig.empty:
            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
            html += '<th>Strategy</th><th>Target</th><th>Ticker</th>'
            html += '<th>Price</th><th>TP Price</th><th>매도기한</th><th>실제승률</th><th>BT승률</th>'
            html += '</tr></thead><tbody>'
            for _, row in new_sig.iterrows():
                s_key = safe_str(row.get('strategy'))
                tk = safe_str(row.get('ticker'))
                pr = safe_float(row.get('price', 0))
                tp = safe_float(row.get('tp_price', 0))
                sig_date = safe_str(row.get('date'))
                mh = STRAT_MAX_HOLD.get(s_key, 5)
                pr_h = f'${pr:.2f}' if pr > 0 else '—'
                tp_h = f'<span style="color:var(--gold);font-weight:600">${tp:.2f}</span>' if tp > 0 else '—'
                sell_by = calc_sell_date_kst(sig_date, mh)
                _awr = _actual_wr.get(s_key, '—')
                html += f'<tr>'
                html += f'<td>{stag(s_key)} <span style="color:var(--text-muted);font-size:0.85em">{STRAT_KR.get(s_key,"")}</span></td>'
                html += f'<td style="color:var(--gold);font-weight:700;font-size:1.05em">{STRAT_TP.get(s_key,"")}</td>'
                html += f'<td style="font-weight:700;color:var(--text-primary);font-size:1.05em">{tk}</td>'
                html += f'<td>{pr_h}</td>'
                html += f'<td>{tp_h}</td>'
                html += f'<td style="color:var(--red-bright);font-weight:600">{sell_by} <span style="font-size:0.8em;color:var(--text-muted)">({mh}일)</span></td>'
                html += f'<td style="color:var(--green-bright);font-weight:600">{_awr}</td>'
                html += f'<td style="color:var(--text-muted)">{STRAT_BT_WR.get(s_key,"—")}</td>'
                html += f'</tr>'
            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown('<div class="rr-empty">오늘 New Strategy 신호 없음</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="rr-empty">오늘 감지된 신호가 없습니다</div>', unsafe_allow_html=True)

# ─── Tab: Performance ─────────────────────────────────────────────────────────
with tab_history:
    open_pos = load_open_positions()
    closed_pos = load_closed_positions()

    # Data prep
    all_records = pd.DataFrame()
    frames = []
    if not open_pos.empty and 'strategy' in open_pos.columns:
        tmp = open_pos[['strategy', 'signal_date', 'status']].copy()
        tmp.rename(columns={'status': 'result'}, inplace=True)
        frames.append(tmp)
    if not closed_pos.empty and 'strategy' in closed_pos.columns:
        rc = 'result_status' if 'result_status' in closed_pos.columns else 'status'
        tmp = closed_pos[['strategy', 'signal_date', rc]].copy()
        tmp.rename(columns={rc: 'result'}, inplace=True)
        frames.append(tmp)
    if frames:
        all_records = pd.concat(frames, ignore_index=True)
        all_records['signal_date'] = pd.to_datetime(all_records['signal_date'], errors='coerce')
        all_records = all_records.dropna(subset=['signal_date'])
        # 미국 거래일 → KST 날짜로 변환하여 표시
        all_records['_sd_str'] = all_records['signal_date'].dt.strftime('%Y-%m-%d')
        all_records['date_str'] = all_records['_sd_str'].apply(us_to_kst_short)
        all_records['month_str'] = all_records['_sd_str'].apply(
            lambda x: us_to_kst_date(x)[:7] if us_to_kst_date(x) != '—' else '—'
        )

    strategies = list(STRAT_ORDER)
    if not all_records.empty and 'strategy' in all_records.columns:
        extra = sorted([s for s in all_records['strategy'].unique() if s not in strategies], key=lambda x: int(x) if x.isdigit() else 999)
        strategies.extend(extra)
    rs_col = 'result_status' if (not closed_pos.empty and 'result_status' in closed_pos.columns) else 'status'

    # Aggregate — 대소문자 무관 매칭
    # v4: result_status 가능값은 WIN / LOSS / EXPIRED 3종. TRAILING 집계 제거.
    total_det = len(all_records)
    if not closed_pos.empty and rs_col in closed_pos.columns:
        _rs_upper = closed_pos[rs_col].astype(str).str.upper()
        total_closed = len(closed_pos)
        total_win = int((_rs_upper == 'WIN').sum())
        total_loss = int((_rs_upper == 'LOSS').sum())
        total_exp = int((_rs_upper == 'EXPIRED').sum())
    else:
        total_closed = total_win = total_loss = total_exp = 0
    total_fail = total_loss + total_exp  # LOSS + EXPIRED 모두 실패로 집계
    total_open = len(open_pos[open_pos['status'].str.upper().isin(['OPEN', 'PENDING'])]) if not open_pos.empty and 'status' in open_pos.columns else 0
    # 승률: Win / (Win + Loss + Expired) — LOSS·EXPIRED는 실패로 간주
    total_decided = total_win + total_fail
    wr = (total_win / total_decided * 100) if total_decided > 0 else 0

    # Summary cards — 합계 검증: Detected = Active + Win + Loss/Exp
    st.markdown(f'''<div class="rr-cards">
        <div class="rr-card"><div class="val" style="color:var(--text-secondary)">{total_det}</div><div class="lbl">Detected</div></div>
        <div class="rr-card"><div class="val" style="color:var(--blue)">{total_open}</div><div class="lbl">Active</div></div>
        <div class="rr-card"><div class="val" style="color:var(--green-bright)">{total_win}</div><div class="lbl">Wins</div></div>
        <div class="rr-card"><div class="val" style="color:var(--red-bright)">{total_fail}</div><div class="lbl">Loss / Exp</div></div>
        <div class="rr-card accent"><div class="val" style="color:var(--gold)">{wr:.1f}%</div><div class="lbl">Win Rate</div></div>
    </div>''', unsafe_allow_html=True)

    # Sub tabs
    h_daily, h_monthly, h_winrate, h_active, h_closed_detail, h_ticker, h_pnl, h_analytics, h_dq, h_risk = st.tabs([
        "Daily", "Monthly", "Strategy", "Active", "Closed", "By Ticker", "P&L", "Analytics", "Data Quality", "Risk"
    ])

    # ── 1) Daily Matrix ──
    with h_daily:
        if all_records.empty:
            st.markdown('<div class="rr-empty">No data yet — run scanner first</div>', unsafe_allow_html=True)
        else:
            cutoff = all_records['signal_date'].max() - pd.Timedelta(days=30)
            recent = all_records[all_records['signal_date'] >= cutoff].copy()
            # Sort by actual date, not by mm/dd string (avoids year-boundary bug)
            _date_map = recent.drop_duplicates('date_str').set_index('date_str')['signal_date']
            date_order = sorted(recent['date_str'].unique(), key=lambda x: _date_map.get(x, pd.Timestamp.min))

            det_g = recent.groupby(['strategy','date_str']).size().unstack(fill_value=0)
            wins_r = recent[recent['result']=='WIN']
            ach_g = wins_r.groupby(['strategy','date_str']).size().unstack(fill_value=0) if not wins_r.empty else pd.DataFrame(0,index=strategies,columns=date_order)
            loss_r = recent[recent['result'].isin(['LOSS','EXPIRED'])]
            loss_g = loss_r.groupby(['strategy','date_str']).size().unstack(fill_value=0) if not loss_r.empty else pd.DataFrame(0,index=strategies,columns=date_order)
            prog_r = recent[recent['result'].isin(['OPEN','PENDING'])]
            prog_g = prog_r.groupby(['strategy','date_str']).size().unstack(fill_value=0) if not prog_r.empty else pd.DataFrame(0,index=strategies,columns=date_order)

            for s in strategies:
                for g in [det_g,ach_g,loss_g,prog_g]:
                    if s not in g.index: g.loc[s]=0
            for d in date_order:
                for g in [det_g,ach_g,loss_g,prog_g]:
                    if d not in g.columns: g[d]=0

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr><th>Strategy</th>'
            for d in date_order: html += f'<th>{d}</th>'
            html += '</tr></thead><tbody>'
            for s in strategies:
                html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td>'
                for d in date_order:
                    dv = int(det_g.loc[s,d]) if d in det_g.columns else 0
                    av = int(ach_g.loc[s,d]) if d in ach_g.columns else 0
                    lv = int(loss_g.loc[s,d]) if d in loss_g.columns else 0
                    pv = int(prog_g.loc[s,d]) if d in prog_g.columns else 0
                    html += f'<td>{cell_html(av,dv,lv,pv)}</td>'
                html += '</tr>'
            html += '</tbody></table></div>'

            st.markdown(f'''<div class="rr-legend">
                Last 30 days — <span class="c-win">Green = All TP</span> ·
                <span class="c-loss">Red = Loss</span> ·
                <span class="c-partial">Amber = Partial</span> ·
                <span class="c-pending">Gray = Active</span>
            </div>{html}''', unsafe_allow_html=True)

    # ── 2) Monthly Matrix ──
    with h_monthly:
        if all_records.empty:
            st.markdown('<div class="rr-empty">No data yet</div>', unsafe_allow_html=True)
        else:
            mo = sorted(all_records['month_str'].unique())
            m_det = all_records.groupby(['strategy','month_str']).size().unstack(fill_value=0)
            m_win = all_records[all_records['result']=='WIN']
            m_ach = m_win.groupby(['strategy','month_str']).size().unstack(fill_value=0) if not m_win.empty else pd.DataFrame(0,index=strategies,columns=mo)
            m_l = all_records[all_records['result'].isin(['LOSS','EXPIRED'])]
            m_lg = m_l.groupby(['strategy','month_str']).size().unstack(fill_value=0) if not m_l.empty else pd.DataFrame(0,index=strategies,columns=mo)
            m_p = all_records[all_records['result'].isin(['OPEN','PENDING'])]
            m_pg = m_p.groupby(['strategy','month_str']).size().unstack(fill_value=0) if not m_p.empty else pd.DataFrame(0,index=strategies,columns=mo)

            for s in strategies:
                for g in [m_det,m_ach,m_lg,m_pg]:
                    if s not in g.index: g.loc[s]=0
            for m in mo:
                for g in [m_det,m_ach,m_lg,m_pg]:
                    if m not in g.columns: g[m]=0

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr><th>Strategy</th>'
            for m in mo: html += f'<th>{m}</th>'
            html += '</tr></thead><tbody>'
            for s in strategies:
                html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td>'
                for m in mo:
                    dv=int(m_det.loc[s,m]) if m in m_det.columns else 0
                    av=int(m_ach.loc[s,m]) if m in m_ach.columns else 0
                    lv=int(m_lg.loc[s,m]) if m in m_lg.columns else 0
                    pv=int(m_pg.loc[s,m]) if m in m_pg.columns else 0
                    html += f'<td>{cell_html(av,dv,lv,pv)}</td>'
                html += '</tr>'
            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)

    # ── 3) Strategy Win Rates ──
    with h_winrate:
        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
        html += '<th>Strategy</th><th>Target</th><th>Total</th><th>Closed</th>'
        html += '<th>Win</th><th>Loss</th><th>Expired</th><th>Active</th>'
        html += '<th>Win Rate</th><th>Backtest</th><th>Avg P&L</th>'
        html += '</tr></thead><tbody>'

        for s in strategies:
            s_all = all_records[all_records['strategy']==s] if not all_records.empty else pd.DataFrame()
            s_cl = closed_pos[closed_pos['strategy']==s] if not closed_pos.empty and 'strategy' in closed_pos.columns else pd.DataFrame()
            s_op = open_pos[(open_pos['strategy']==s)&(open_pos['status'].isin(['OPEN','PENDING']))] if not open_pos.empty and 'strategy' in open_pos.columns and 'status' in open_pos.columns else pd.DataFrame()

            nd=len(s_all); nc=len(s_cl)
            _s_rs = s_cl[rs_col].astype(str).str.upper() if nc>0 else pd.Series(dtype=str)
            nw = int((_s_rs=='WIN').sum()) if nc>0 else 0
            nl = int((_s_rs=='LOSS').sum()) if nc>0 else 0
            ne = int((_s_rs=='EXPIRED').sum()) if nc>0 else 0
            na = len(s_op)
            # v4: 승률 = Win / (Win + Loss + Expired). LOSS·EXPIRED는 실패로 간주.
            s_decided = nw + nl + ne
            sw = (nw/s_decided*100) if s_decided>0 else 0
            avg = 0
            if nc>0 and 'result_pct' in s_cl.columns:
                avg = safe_float(pd.to_numeric(s_cl['result_pct'],errors='coerce').mean())

            wrc = 'c-win' if sw>=80 else 'c-partial' if sw>0 else 'c-none'
            rc = 'c-win' if avg>0 else 'c-loss' if avg<0 else 'c-none'

            html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td>'
            html += f'<td style="color:var(--gold);font-weight:600">{STRAT_TP.get(s,"")}</td>'
            html += f'<td>{nd}</td><td>{nc if nc>0 else "—"}</td>'
            html += f'<td class="c-win">{nw if nw>0 else "—"}</td>'
            html += f'<td class="{"c-loss" if nl>0 else "c-none"}">{nl if nl>0 else "—"}</td>'
            html += f'<td class="{"c-partial" if ne>0 else "c-none"}">{ne if ne>0 else "—"}</td>'
            html += f'<td class="{"st-open" if na>0 else "c-none"}">{na if na>0 else "—"}</td>'
            html += f'<td class="{wrc}">{sw:.1f}%</td>' if nc>0 else '<td class="c-none">—</td>'
            html += f'<td class="c-muted">{STRAT_BT_WR.get(s,"—")}</td>'
            html += f'<td class="{rc}">{avg:+.1f}%</td>' if nc>0 else '<td class="c-none">—</td>'
            html += '</tr>'

        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)

    # ── 4) Active Positions ──
    with h_active:
        active = pd.DataFrame()
        if not open_pos.empty and 'status' in open_pos.columns:
            active = open_pos[open_pos['status'].isin(['PENDING','OPEN'])]
        if not active.empty:
            fa1, fa2, _ = st.columns([2,2,6])
            with fa1:
                astrats = sorted(active['strategy'].dropna().unique()) if 'strategy' in active.columns else []
                af = st.selectbox('Strategy', ['All']+astrats, key='af')
            with fa2:
                asort = st.selectbox('Sort', ['Date (New)','Date (Old)','P&L High','P&L Low','Ach High'], key='as')
            if af != 'All': active = active[active['strategy']==af]
            if asort=='Date (New)': active=active.sort_values('signal_date',ascending=False)
            elif asort=='Date (Old)': active=active.sort_values('signal_date',ascending=True)
            elif asort in ('P&L High','P&L Low'):
                active['_s']=pd.to_numeric(active.get('change_pct',0),errors='coerce').fillna(0)
                active=active.sort_values('_s',ascending=(asort=='P&L Low'))
            elif asort=='Ach High':
                active['_s']=pd.to_numeric(active.get('achievement_pct',0),errors='coerce').fillna(0)
                active=active.sort_values('_s',ascending=False)

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
            html += '<th>Strat</th><th>Ticker</th><th>Signal</th><th>매도기한</th>'
            html += '<th>Price</th><th>Current</th><th>P&L</th>'
            html += '<th>TP</th><th>Peak</th><th>Peak Date</th>'
            html += '<th>Achievement</th><th>Remaining</th>'
            html += '<th>Days</th><th>Status</th>'
            html += '</tr></thead><tbody>'

            for _, row in active.iterrows():
                s=safe_str(row.get('strategy')); tk=safe_str(row.get('ticker'))
                sig_dt=us_to_kst_short(safe_str(row.get('signal_date'))); status=safe_str(row.get('status'))
                is_p = (status=='PENDING')
                sp = safe_float(safe_str(row.get('signal_price')))
                ep_raw=safe_str(row.get('entry_price')); cp_raw=safe_str(row.get('current_price'))
                tp_raw=safe_str(row.get('tp_price')); mx_raw=safe_str(row.get('max_price'))
                mx_dt=us_to_kst_short(safe_str(row.get('max_price_date')))
                ach_raw=safe_str(row.get('achievement_pct')); chg_raw=safe_str(row.get('change_pct'))
                dh=safe_str(row.get('days_held')); mh_raw=safe_str(row.get('max_hold'))
                mh = mh_raw if mh_raw!='—' else str(STRAT_MAX_HOLD.get(s,''))

                # 매도기한 계산 (signal_date + max_hold 영업일)
                _sig_raw = safe_str(row.get('signal_date'))
                _mh_i = int(safe_float(mh)) if mh != '—' else STRAT_MAX_HOLD.get(s, 5)
                sell_by_h = calc_sell_date_kst(_sig_raw, _mh_i)

                if is_p:
                    ep_h=f'<span class="c-muted">${sp:.2f}</span>' if sp>0 else '<span class="c-muted">—</span>'
                    cp_h=ep_h; chg_h='<span class="c-muted">WAIT</span>'
                    mx_h='<span class="c-muted">—</span>'; mx_dt_h='<span class="c-muted">—</span>'
                    ach_h='<span class="c-muted">—</span>'
                    tp_f=safe_float(tp_raw)
                    rem_h = f'<span class="c-muted">~{STRAT_TP.get(s,"")}</span>'
                    days_h=f'<span class="c-muted">0/{mh}</span>'
                else:
                    if ep_raw=='—' and sp>0: ep_raw=f'{sp:.2f}'
                    ep_h = f'${ep_raw}' if ep_raw!='—' else '—'
                    if cp_raw=='—':
                        fb=safe_float(ep_raw) if ep_raw!='—' else sp
                        cp_h=f'<span class="c-muted">${fb:.2f}</span>' if fb>0 else '—'
                        cp_raw=str(fb) if fb>0 else '—'
                    else: cp_h=f'${cp_raw}'
                    tp_h=f'${tp_raw}' if tp_raw!='—' else '—'
                    mx_h=f'${mx_raw}' if mx_raw!='—' else '<span class="c-muted">—</span>'
                    mx_dt_h=mx_dt
                    chg_h=chg_html(chg_raw)

                    ach_val=safe_float(ach_raw)
                    ac='var(--green-bright)' if ach_val>=100 else 'var(--amber)' if ach_val>=50 else 'var(--red-bright)'
                    ach_h=progress_bar(ach_val,ac) if ach_raw!='—' and ach_val>0 else '<span class="c-muted">—</span>'

                    cf=safe_float(cp_raw); tf=safe_float(tp_raw)
                    if cf>0 and tf>0:
                        rp=(tf-cf)/cf*100
                        rem_h=f'<span style="color:var(--blue);font-weight:500">+{rp:.1f}%</span>' if rp>0 else '<span class="c-win">HIT</span>'
                    else: rem_h='—'
                    # Expiration warning: color days based on usage ratio
                    dh_i = safe_float(dh); mh_i = safe_float(mh)
                    if dh!='—' and mh_i > 0:
                        usage = dh_i / mh_i
                        if usage >= 0.8:
                            days_h = f'<span style="color:var(--red-bright);font-weight:700">{dh}/{mh}</span>'
                        elif usage >= 0.6:
                            days_h = f'<span style="color:var(--amber);font-weight:600">{dh}/{mh}</span>'
                        else:
                            days_h = f'{dh}/{mh}'
                    else:
                        days_h = f'{dh}/{mh}' if dh!='—' else '—'

                st_cls = 'st-open' if status=='OPEN' else 'st-pending'
                st_txt = 'OPEN' if status=='OPEN' else 'WAIT'

                html += f'<tr><td>{stag(s)}</td><td style="font-weight:600;color:var(--text-primary)">{tk}</td>'
                html += f'<td>{sig_dt}</td><td style="color:var(--red-bright);font-weight:600">{sell_by_h}</td><td>{ep_h}</td><td>{cp_h}</td><td>{chg_h}</td>'
                html += f'<td>{tp_raw if tp_raw!="—" else "—"}</td><td>{mx_h}</td><td>{mx_dt_h}</td>'
                html += f'<td>{ach_h}</td><td>{rem_h}</td><td>{days_h}</td>'
                html += f'<td class="{st_cls}">{st_txt}</td></tr>'

            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)
            st.markdown('<div class="rr-legend">Achievement = (Peak - Entry) / (TP - Entry) × 100 | Remaining = % from current to TP</div>', unsafe_allow_html=True)

            # Expanders for detail
            for _, row in active.iterrows():
                tk=safe_str(row.get('ticker')); s=safe_str(row.get('strategy'))
                status=safe_str(row.get('status')); st_kr='OPEN' if status=='OPEN' else 'WAIT'
                with st.expander(f"[{STRAT_TAB.get(s, s)}] {tk} — {st_kr}", expanded=False):
                    d1,d2,d3=st.columns(3)
                    sp=safe_float(safe_str(row.get('signal_price')))
                    ep=safe_float(safe_str(row.get('entry_price')))
                    cp=safe_float(safe_str(row.get('current_price')))
                    tp=safe_float(safe_str(row.get('tp_price')))
                    sl=safe_float(safe_str(row.get('sl_price')))
                    mx=safe_float(safe_str(row.get('max_price')))
                    mn=safe_float(safe_str(row.get('min_price')))
                    cv=safe_float(safe_str(row.get('change_pct')))
                    av=safe_float(safe_str(row.get('achievement_pct')))
                    dh=safe_str(row.get('days_held'))
                    mh_r=safe_str(row.get('max_hold'))
                    mh=mh_r if mh_r!='—' else str(STRAT_MAX_HOLD.get(s,''))
                    with d1:
                        st.markdown(f"**Signal**\n- Strategy: {STRAT_TAB.get(s,s)} ({STRAT_KR.get(s,'')})\n- Date: {us_to_kst_short(safe_str(row.get('signal_date')))}\n- Price: ${sp:.2f}" if sp>0 else f"**Signal**\n- Strategy: {STRAT_TAB.get(s,s)}\n- Date: {us_to_kst_short(safe_str(row.get('signal_date')))}\n- Price: —")
                    with d2:
                        if status=='PENDING':
                            st.markdown(f"**Entry**\n- Status: Waiting\n- TP: ${tp:.2f}" + (f"\n- SL: ${sl:.2f}" if sl>0 else "\n- SL: None"))
                        else:
                            st.markdown(f"**Position**\n- Entry: ${ep:.2f}\n- Current: ${cp:.2f}\n- P&L: {cv:+.1f}%")
                    with d3:
                        if status!='PENDING':
                            st.markdown(f"**Tracking**\n- Days: {dh}/{mh}\n- Peak: ${mx:.2f}\n- Low: ${mn:.2f}\n- Achievement: {av:.0f}%")
                        else:
                            st.markdown(f"**Tracking**\n- Days: 0/{mh}\n- Target: {STRAT_TP.get(s,'')}")
        else:
            st.markdown('<div class="rr-empty">No active positions</div>', unsafe_allow_html=True)

    # ── 5) Closed Positions ──
    with h_closed_detail:
        if not closed_pos.empty and 'strategy' in closed_pos.columns:
            cp_s = closed_pos.copy()
            fc1,fc2,fc3,_=st.columns([2,2,2,4])
            with fc1:
                cs=sorted(cp_s['strategy'].dropna().unique())
                cf=st.selectbox('Strategy',['All']+list(cs),key='cf')
            with fc2:
                ro=['All']
                if rs_col in cp_s.columns: ro+=sorted(cp_s[rs_col].dropna().unique())
                cr=st.selectbox('Result',ro,key='cr')
            with fc3:
                cso=st.selectbox('Sort',['Close (New)','Close (Old)','P&L High','P&L Low'],key='cso')
            if cf!='All': cp_s=cp_s[cp_s['strategy']==cf]
            if cr!='All': cp_s=cp_s[cp_s[rs_col]==cr]
            cp_s['_cd']=pd.to_datetime(cp_s.get('close_date',''),errors='coerce')
            if cso=='Close (New)': cp_s=cp_s.sort_values('_cd',ascending=False)
            elif cso=='Close (Old)': cp_s=cp_s.sort_values('_cd',ascending=True)
            elif cso in ('P&L High','P&L Low'):
                cp_s['_rp']=pd.to_numeric(cp_s.get('result_pct',0),errors='coerce').fillna(0)
                cp_s=cp_s.sort_values('_rp',ascending=(cso=='P&L Low'))

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
            html += '<th>Strat</th><th>Ticker</th><th>Signal</th><th>Entry</th><th>Price</th>'
            html += '<th>Result</th><th>Close</th><th>Close $</th><th>P&L</th>'
            html += '<th>TP Hit</th><th>Peak</th><th>Peak Date</th><th>Ach%</th>'
            html += '</tr></thead><tbody>'

            for _, row in cp_s.iterrows():
                s=safe_str(row.get('strategy')); tk=safe_str(row.get('ticker'))
                sig_dt=us_to_kst_short(safe_str(row.get('signal_date'))); ent_dt=us_to_kst_short(safe_str(row.get('entry_date')))
                ent_pr=safe_str(row.get('entry_price')); result=safe_str(row.get(rs_col,'status'))
                close_dt=us_to_kst_short(safe_str(row.get('close_date'))); close_pr=safe_str(row.get('close_price'))
                rp_raw=safe_str(row.get('result_pct')); tp_hit=us_to_kst_short(safe_str(row.get('tp_hit_date')))
                mx=safe_str(row.get('max_price')); mx_dt=us_to_kst_short(safe_str(row.get('max_price_date')))
                ma_raw=safe_str(row.get('max_achievement_pct'))

                res_h=result_badge(result)
                rp_h=chg_html(rp_raw)

                tp_h = f'<span class="c-win">{tp_hit}</span>' if tp_hit!='—' else '<span class="c-muted">—</span>'

                if tp_hit=='—' and mx!='—':
                    mx_h=f'<span style="color:var(--amber);font-weight:600">${mx}</span>'
                    mx_dt_h=f'<span style="color:var(--amber)">{mx_dt}</span>'
                else:
                    mx_h=f'${mx}' if mx!='—' else '—'
                    mx_dt_h=mx_dt

                ma_val=safe_float(ma_raw)
                if ma_raw!='—' and ma_val>0:
                    ac='var(--green-bright)' if ma_val>=100 else 'var(--amber)' if ma_val>=50 else 'var(--red-bright)'
                    ma_h=f'<span style="color:{ac};font-weight:600">{ma_val:.0f}%</span>'
                else: ma_h='—'

                ep_h=f'${ent_pr}' if ent_pr!='—' else '—'
                cp_h=f'${close_pr}' if close_pr!='—' else '—'

                html += f'<tr><td>{stag(s)}</td><td style="font-weight:600">{tk}</td>'
                html += f'<td>{sig_dt}</td><td>{ent_dt}</td><td>{ep_h}</td>'
                html += f'<td>{res_h}</td><td>{close_dt}</td><td>{cp_h}</td><td>{rp_h}</td>'
                html += f'<td>{tp_h}</td><td>{mx_h}</td><td>{mx_dt_h}</td><td>{ma_h}</td></tr>'

            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown('<div class="rr-empty">No closed positions</div>', unsafe_allow_html=True)

    # ── 6) By Ticker ──
    with h_ticker:
        tk_rows=[]
        if not closed_pos.empty and 'ticker' in closed_pos.columns:
            for _, row in closed_pos.iterrows():
                result=safe_str(row.get(rs_col,'status'))
                tk_rows.append({'ticker':safe_str(row.get('ticker')),'strategy':safe_str(row.get('strategy')),
                    'signal_date':safe_str(row.get('signal_date')),'entry_date':safe_str(row.get('entry_date')),
                    'entry_price':safe_str(row.get('entry_price')),'tp_price':safe_str(row.get('tp_price')),
                    'current_price':safe_str(row.get('close_price')),'max_price':safe_str(row.get('max_price')),
                    'max_price_date':safe_str(row.get('max_price_date')),'result':result,
                    'result_pct':safe_str(row.get('result_pct')),'tp_hit_date':safe_str(row.get('tp_hit_date')),
                    'max_ach':safe_str(row.get('max_achievement_pct')),'close_date':safe_str(row.get('close_date')),
                    'is_open':False,'signal_price':'—'})
        if not open_pos.empty and 'ticker' in open_pos.columns:
            for _, row in open_pos.iterrows():
                status=safe_str(row.get('status'))
                tk_rows.append({'ticker':safe_str(row.get('ticker')),'strategy':safe_str(row.get('strategy')),
                    'signal_date':safe_str(row.get('signal_date')),'entry_date':safe_str(row.get('entry_date')),
                    'entry_price':safe_str(row.get('entry_price')),'signal_price':safe_str(row.get('signal_price')),
                    'tp_price':safe_str(row.get('tp_price')),'current_price':safe_str(row.get('current_price')),
                    'max_price':safe_str(row.get('max_price')),'max_price_date':safe_str(row.get('max_price_date')),
                    'result':status,'result_pct':safe_str(row.get('change_pct')),'tp_hit_date':'—',
                    'max_ach':safe_str(row.get('achievement_pct')),'close_date':'—','is_open':True})

        if not tk_rows:
            st.markdown('<div class="rr-empty">No tracked tickers</div>', unsafe_allow_html=True)
        else:
            ft1,ft2,_=st.columns([2,2,6])
            with ft1:
                tks=sorted(set(r['strategy'] for r in tk_rows if r['strategy']!='—'))
                tf=st.selectbox('Strategy',['All']+tks,key='tf')
            with ft2:
                ts=st.selectbox('Sort',['Recent','Name','Count','Win Rate'],key='ts')
            filtered = tk_rows if tf=='All' else [r for r in tk_rows if r['strategy']==tf]
            tg=defaultdict(list)
            for r in filtered:
                if r['ticker']!='—': tg[r['ticker']].append(r)
            for t in tg: tg[t].sort(key=lambda x:x['signal_date'],reverse=True)

            if ts=='Recent': st_list=sorted(tg.items(),key=lambda x:x[1][0]['signal_date'] if x[1] else '',reverse=True)
            elif ts=='Name': st_list=sorted(tg.items(),key=lambda x:x[0])
            elif ts=='Count': st_list=sorted(tg.items(),key=lambda x:len(x[1]),reverse=True)
            elif ts=='Win Rate':
                def _wr(item):
                    w=sum(1 for p in item[1] if p['result']=='WIN')
                    c=sum(1 for p in item[1] if p['result'] in ('WIN','LOSS','EXPIRED'))
                    return (w/c*100) if c>0 else 0
                st_list=sorted(tg.items(),key=_wr,reverse=True)
            else: st_list=sorted(tg.items(),key=lambda x:x[1][0]['signal_date'] if x[1] else '',reverse=True)

            st.markdown(f'<div class="rr-legend">{len(st_list)} tickers</div>', unsafe_allow_html=True)

            html=''
            for tk,positions in st_list:
                nt=len(positions)
                nw=sum(1 for p in positions if p['result']=='WIN')
                nl=sum(1 for p in positions if p['result']=='LOSS')
                ne=sum(1 for p in positions if p['result']=='EXPIRED')
                na=sum(1 for p in positions if p['result'] in ('OPEN','PENDING'))
                nc=nw+nl+ne
                tw=(nw/nc*100) if nc>0 else 0
                badges=' '.join(stag(s_) for s_ in sorted(set(p['strategy'] for p in positions if p['strategy']!='—')))
                wc='var(--green-bright)' if tw>=80 else 'var(--amber)' if tw>0 else 'var(--text-muted)'

                parts=[]
                if nw>0: parts.append(f'<span class="c-win">{nw}W</span>')
                if nl>0: parts.append(f'<span class="c-loss">{nl}L</span>')
                if ne>0: parts.append(f'<span class="c-partial">{ne}E</span>')
                if na>0: parts.append(f'<span class="st-open">{na}A</span>')
                sm=' / '.join(parts) if parts else '—'

                html += f'''<div class="tk-header">
                    <div style="display:flex;align-items:center;gap:10px">
                        <span class="tk-name">{tk}</span>{badges}
                    </div>
                    <div class="tk-meta"><span>{nt}</span><span>{sm}</span>
                        <span style="color:{wc};font-weight:600">{tw:.0f}%</span>
                    </div>
                </div>'''

                html += '<table class="rr-table" style="margin-bottom:0"><thead><tr>'
                html += '<th>Strat</th><th>Signal</th><th>Entry</th><th>Price</th>'
                html += '<th>Result</th><th>P&L</th><th>TP Hit</th><th>Peak</th><th>Peak Date</th><th>Ach%</th>'
                html += '</tr></thead><tbody>'

                for p in positions:
                    s_=p['strategy']; res=p['result']; rp=p['result_pct']
                    tp_hit=us_to_kst_short(p['tp_hit_date']); mx=p['max_price']; mx_dt=us_to_kst_short(p['max_price_date']); ma=p['max_ach']
                    ep=p['entry_price']; sig_pr=p.get('signal_price','—'); sp_f=safe_float(sig_pr)
                    is_pend=(res=='PENDING')

                    if is_pend:
                        ed_h='<span class="c-muted">WAIT</span>'
                        ep_h=f'<span class="c-muted">${sp_f:.2f}</span>' if sp_f>0 else '<span class="c-muted">—</span>'
                        rp_h='<span class="c-muted">—</span>'; tp_h='<span class="c-muted">—</span>'
                        mx_h='<span class="c-muted">—</span>'; mxd_h='<span class="c-muted">—</span>'
                        ach_h='<span class="c-muted">—</span>'
                    else:
                        ed_h=us_to_kst_short(p['entry_date']) if p['entry_date']!='—' else '<span class="c-muted">—</span>'
                        if ep=='—' and sp_f>0: ep=f'{sp_f:.2f}'
                        ep_h=f'${ep}' if ep!='—' else '<span class="c-muted">—</span>'
                        rp_h=chg_html(rp)
                        tp_h=f'<span class="c-win">{tp_hit}</span>' if tp_hit!='—' else '<span class="c-muted">—</span>'
                        if mx!='—':
                            if tp_hit=='—':
                                mx_h=f'<span style="color:var(--amber);font-weight:600">${mx}</span>'
                                mxd_h=f'<span style="color:var(--amber)">{mx_dt}</span>'
                            else: mx_h=f'${mx}'; mxd_h=mx_dt
                        else: mx_h='<span class="c-muted">—</span>'; mxd_h='<span class="c-muted">—</span>'
                        mav=safe_float(ma)
                        if ma!='—' and mav>0:
                            ac='var(--green-bright)' if mav>=100 else 'var(--amber)' if mav>=50 else 'var(--red-bright)'
                            ach_h=progress_bar(mav,ac)
                        else: ach_h='<span class="c-muted">—</span>'

                    html += f'<tr><td>{stag(s_)}</td><td>{us_to_kst_short(p["signal_date"])}</td><td>{ed_h}</td><td>{ep_h}</td>'
                    html += f'<td>{result_badge(res)}</td><td>{rp_h}</td><td>{tp_h}</td>'
                    html += f'<td>{mx_h}</td><td>{mxd_h}</td><td>{ach_h}</td></tr>'
                html += '</tbody></table>'

            st.markdown(html, unsafe_allow_html=True)

    # ── 7) P&L Chart ──
    with h_pnl:
        if closed_pos.empty or 'close_date' not in closed_pos.columns or 'result_pct' not in closed_pos.columns:
            st.markdown('<div class="rr-empty">No closed positions for P&L</div>', unsafe_allow_html=True)
        else:
            pnl_df = closed_pos[['strategy','close_date','result_pct']].copy()
            pnl_df['result_pct']=pd.to_numeric(pnl_df['result_pct'],errors='coerce')
            pnl_df=pnl_df.dropna(subset=['result_pct','close_date'])
            pnl_df['close_date']=pd.to_datetime(pnl_df['close_date'],errors='coerce')
            pnl_df=pnl_df.dropna(subset=['close_date']).sort_values('close_date')

            if pnl_df.empty:
                st.markdown('<div class="rr-empty">No valid P&L data</div>', unsafe_allow_html=True)
            else:
                strats_data=sorted(pnl_df['strategy'].unique())
                sc={'1':'#4a9e7d','2':'#4a7a9e','3':'#b8954a','4':'#9e4a5a','5':'#7a5aaf','6':'#c08820','7':'#309a8a','8':'#b04858','9':'#5088c0','10':'#8868b8'}

                chart_data=pd.DataFrame()
                for s in strats_data:
                    sd=pnl_df[pnl_df['strategy']==s].copy().sort_values('close_date')
                    sd[s]=sd['result_pct'].cumsum()
                    daily=sd.groupby('close_date')[s].last()
                    chart_data=daily.to_frame() if chart_data.empty else chart_data.join(daily,how='outer')

                total=pnl_df.sort_values('close_date').copy()
                total['Total']=total['result_pct'].cumsum()
                td=total.groupby('close_date')['Total'].last()
                chart_data=chart_data.join(td,how='outer').sort_index().ffill().fillna(0)

                def calc_mdd(cs):
                    # Prepend 0 as starting point so initial losses count as drawdown
                    start_idx = cs.index[0] - pd.Timedelta(days=1) if len(cs) > 0 else cs.index[0]
                    cs0 = pd.concat([pd.Series([0.0], index=[start_idx]), cs])
                    pk=cs0.cummax(); dd=cs0-pk; mv=dd.min()
                    if mv==0: return 0,None,None,pd.Series(0,index=cs.index)
                    me=dd.idxmin(); ms=cs0.loc[:me].idxmax()
                    # Return dd aligned to original index (drop the prepended zero)
                    return mv,ms,me,dd.iloc[1:]

                tc=chart_data['Total'] if 'Total' in chart_data.columns else pd.Series(dtype=float)
                tm,tms,tme,tdd = calc_mdd(tc) if not tc.empty else (0,None,None,pd.Series(dtype=float))

                smdd={}
                for s in strats_data:
                    if s in chart_data.columns:
                        sv,ss,se,sd=calc_mdd(chart_data[s])
                        smdd[s]={'mdd':sv,'start':ss,'end':se,'dd':sd}

                tp=pnl_df['result_pct'].sum(); ap=pnl_df['result_pct'].mean()
                nt=len(pnl_df); nw_profit=len(pnl_df[pnl_df['result_pct']>0])
                tw=(nw_profit/nt*100) if nt>0 else 0
                tc_='var(--green-bright)' if tp>0 else 'var(--red-bright)' if tp<0 else 'var(--text-muted)'
                mc_='var(--red-bright)' if tm<-5 else 'var(--amber)' if tm<0 else 'var(--text-muted)'
                wc_='var(--green-bright)' if tw>=80 else 'var(--amber)' if tw>0 else 'var(--text-muted)'

                st.markdown(f'''<div class="rr-stats">
                    <div class="rr-stat"><div class="s-label">Cumulative P&L</div><div class="s-value" style="color:{tc_}">{tp:+.1f}%</div></div>
                    <div class="rr-stat"><div class="s-label">Avg per Trade</div><div class="s-value" style="color:{tc_}">{ap:+.2f}%</div></div>
                    <div class="rr-stat"><div class="s-label">Trades</div><div class="s-value" style="color:var(--text-secondary)">{nt}</div></div>
                    <div class="rr-stat"><div class="s-label">Profit Rate</div><div class="s-value" style="color:{wc_}">{tw:.0f}%</div></div>
                    <div class="rr-stat"><div class="s-label">Max Drawdown</div><div class="s-value" style="color:{mc_}">{tm:.1f}%</div></div>
                </div>''', unsafe_allow_html=True)

                if tms is not None and tme is not None:
                    ms_s=tms.strftime('%Y-%m-%d') if hasattr(tms,'strftime') else str(tms)
                    me_s=tme.strftime('%Y-%m-%d') if hasattr(tme,'strftime') else str(tme)
                    st.markdown(f'<div class="rr-legend" style="color:var(--red-bright)">MDD Period: {ms_s} → {me_s} ({tm:.1f}%p)</div>', unsafe_allow_html=True)

                st.markdown('<div class="rr-legend">Cumulative P&L by Strategy + Total</div>', unsafe_allow_html=True)
                cl=[sc.get(c,'#c9a96e') if c!='Total' else '#c9a96e' for c in chart_data.columns]
                st.line_chart(chart_data, color=cl if cl else None)

                st.markdown('<div class="rr-legend" style="margin-top:16px">Drawdown — 0% = Peak, Negative = Loss from Peak</div>', unsafe_allow_html=True)
                dd_c=pd.DataFrame()
                for s in strats_data:
                    if s in smdd: dd_c[s]=smdd[s]['dd']
                if not tdd.empty: dd_c['Total']=tdd
                dd_c=dd_c.sort_index().ffill().fillna(0)
                dcl=[sc.get(c,'#c9a96e') if c!='Total' else '#c9a96e' for c in dd_c.columns]
                st.area_chart(dd_c, color=dcl if dcl else None)

                # Risk summary table
                st.markdown('<div class="rr-legend" style="margin-top:16px">Strategy Risk Summary</div>', unsafe_allow_html=True)
                sh='<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                sh+='<th>Strategy</th><th>Trades</th><th>Win%</th><th>Cum P&L</th><th>Avg</th>'
                sh+='<th>Best</th><th>Worst</th><th style="color:var(--red-bright)">MDD</th><th>MDD Period</th>'
                sh+='</tr></thead><tbody>'

                for s in strats_data:
                    sp=pnl_df[pnl_df['strategy']==s]['result_pct']
                    sn=len(sp); sw=len(sp[sp>0]); sr=(sw/sn*100) if sn>0 else 0
                    st_=sp.sum(); sa=sp.mean(); smx=sp.max(); smn=sp.min()
                    stc='var(--green-bright)' if st_>0 else 'var(--red-bright)' if st_<0 else 'var(--text-muted)'
                    swc='c-win' if sr>=80 else 'c-partial' if sr>0 else 'c-none'
                    si=smdd.get(s,{})
                    sm=si.get('mdd',0); sms=si.get('start'); sme=si.get('end')
                    smc='var(--red-bright)' if sm<-5 else 'var(--amber)' if sm<0 else 'var(--text-muted)'
                    if sms and sme:
                        msr=f'{sms.strftime("%m/%d") if hasattr(sms,"strftime") else str(sms)[:5]}→{sme.strftime("%m/%d") if hasattr(sme,"strftime") else str(sme)[:5]}'
                    else: msr='—'

                    sh+=f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td>{sn}</td>'
                    sh+=f'<td class="{swc}">{sr:.0f}%</td>'
                    sh+=f'<td style="color:{stc};font-weight:600">{st_:+.1f}%</td>'
                    sh+=f'<td style="color:{stc}">{sa:+.2f}%</td>'
                    sh+=f'<td class="c-win">{smx:+.1f}%</td>'
                    sh+=f'<td class="c-loss">{smn:+.1f}%</td>'
                    sh+=f'<td style="color:{smc};font-weight:600">{sm:.1f}%</td>'
                    sh+=f'<td class="c-muted">{msr}</td></tr>'

                sh+='</tbody></table></div>'
                st.markdown(sh, unsafe_allow_html=True)

    # ── 8) Analytics ──
    with h_analytics:
        if closed_pos.empty or len(closed_pos) < 2:
            st.markdown('<div class="rr-empty">Not enough closed positions for analytics</div>', unsafe_allow_html=True)
        else:
            an_cp = closed_pos.copy()
            an_cp['signal_date_dt'] = pd.to_datetime(an_cp['signal_date'], errors='coerce')
            an_cp['entry_date_dt'] = pd.to_datetime(an_cp.get('entry_date', ''), errors='coerce')
            an_cp['close_date_dt'] = pd.to_datetime(an_cp.get('close_date', ''), errors='coerce')
            an_cp['tp_hit_date_dt'] = pd.to_datetime(an_cp.get('tp_hit_date', ''), errors='coerce')
            an_cp['result_pct_f'] = pd.to_numeric(an_cp.get('result_pct', 0), errors='coerce').fillna(0)
            an_cp['days_held_f'] = pd.to_numeric(an_cp.get('days_held', 0), errors='coerce').fillna(0)
            an_cp['signal_price_f'] = pd.to_numeric(an_cp.get('signal_price', 0), errors='coerce').fillna(0)
            an_cp['entry_price_f'] = pd.to_numeric(an_cp.get('entry_price', 0), errors='coerce').fillna(0)
            an_cp['max_ach_f'] = pd.to_numeric(an_cp.get('max_achievement_pct', 0), errors='coerce').fillna(0)
            an_rs = rs_col

            # ─────────────────────────────────────────
            # 1) TIME-TO-TP — TP 도달 소요일 분석
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">TP 도달 속도 (Time-to-TP)</div>', unsafe_allow_html=True)

            wins = an_cp[an_cp[an_rs] == 'WIN'].copy()
            if wins.empty:
                st.markdown('<div class="rr-empty">No WIN positions yet</div>', unsafe_allow_html=True)
            else:
                # days_held_f is already trading days from tracker (close_date==tp_hit_date for WIN)
                # Do NOT use (tp_hit_date - entry_date).dt.days — that gives calendar days (Fri→Mon=3, not 1)
                wins['ttp_days'] = wins['days_held_f']

                html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                html += '<th>Strategy</th><th>Wins</th><th>Avg Days to TP</th><th>Median</th><th>Min</th><th>Max</th><th>1일 내 도달</th>'
                html += '</tr></thead><tbody>'
                for s in strategies:
                    sw = wins[wins['strategy'] == s]
                    if sw.empty:
                        html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td>0</td><td colspan="5" class="c-muted">—</td></tr>'
                        continue
                    ttp = sw['ttp_days']
                    avg_d = ttp.mean(); med_d = ttp.median(); min_d = ttp.min(); max_d = ttp.max()
                    d1 = len(sw[ttp <= 1])
                    d1_pct = (d1 / len(sw) * 100) if len(sw) > 0 else 0
                    spd_color = 'var(--green-bright)' if avg_d <= 2 else 'var(--amber)' if avg_d <= 5 else 'var(--text-secondary)'
                    html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td>'
                    html += f'<td>{len(sw)}</td>'
                    html += f'<td style="color:{spd_color};font-weight:700">{avg_d:.1f}일</td>'
                    html += f'<td>{med_d:.0f}일</td><td>{min_d:.0f}일</td><td>{max_d:.0f}일</td>'
                    html += f'<td style="color:var(--green-bright)">{d1} ({d1_pct:.0f}%)</td></tr>'
                html += '</tbody></table></div>'
                st.markdown(html, unsafe_allow_html=True)
                st.markdown('<div class="rr-legend">빠를수록 자본 회전율이 높아져 복리 효과 극대화</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

            # ─────────────────────────────────────────
            # 2) EXPECTED VALUE — 전략별 기대값
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">기대값 (Expected Value per Trade)</div>', unsafe_allow_html=True)

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
            html += '<th>Strategy</th><th>Win Rate</th><th>Avg Win</th><th>Avg Loss</th><th>EV</th><th>Profit Factor</th>'
            html += '</tr></thead><tbody>'
            for s in strategies:
                sc_ = an_cp[an_cp['strategy'] == s]
                if sc_.empty:
                    html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td colspan="5" class="c-muted">—</td></tr>'
                    continue
                nw_ = len(sc_[sc_[an_rs] == 'WIN']); nl_ = len(sc_[sc_[an_rs].isin(['LOSS', 'EXPIRED'])])
                nc_ = nw_ + nl_
                wr_ = (nw_ / nc_ * 100) if nc_ > 0 else 0
                w_pnl = sc_[sc_[an_rs] == 'WIN']['result_pct_f']
                l_pnl = sc_[sc_[an_rs].isin(['LOSS', 'EXPIRED'])]['result_pct_f']
                avg_w = w_pnl.mean() if len(w_pnl) > 0 else 0
                avg_l = l_pnl.mean() if len(l_pnl) > 0 else 0
                ev = (wr_ / 100) * avg_w + (1 - wr_ / 100) * avg_l
                gross_w = w_pnl.sum() if len(w_pnl) > 0 else 0
                gross_l = abs(l_pnl.sum()) if len(l_pnl) > 0 else 0
                pf = (gross_w / gross_l) if gross_l > 0 else float('inf') if gross_w > 0 else 0
                ev_c = 'var(--green-bright)' if ev > 0 else 'var(--red-bright)' if ev < 0 else 'var(--text-muted)'
                pf_c = 'var(--green-bright)' if pf >= 2 else 'var(--amber)' if pf >= 1 else 'var(--red-bright)'
                pf_s = f'{pf:.2f}' if pf != float('inf') else '∞'
                html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td>'
                html += f'<td>{wr_:.1f}%</td>'
                html += f'<td class="c-win">{avg_w:+.2f}%</td>'
                html += f'<td class="c-loss">{avg_l:+.2f}%</td>'
                html += f'<td style="color:{ev_c};font-weight:700;font-size:1.1em">{ev:+.2f}%</td>'
                html += f'<td style="color:{pf_c};font-weight:600">{pf_s}</td></tr>'
            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)
            st.markdown('<div class="rr-legend">EV = (승률 × 평균수익) + (패률 × 평균손실) | Profit Factor = 총이익 / 총손실 (≥2.0 우수)</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

            # ─────────────────────────────────────────
            # 3) DAY-OF-WEEK ANALYSIS — 요일별 성과
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">요일별 신호 성과 (Day-of-Week)</div>', unsafe_allow_html=True)

            dow_names = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri'}
            an_cp['dow'] = an_cp['signal_date_dt'].dt.dayofweek
            dow_valid = an_cp.dropna(subset=['dow'])

            if dow_valid.empty:
                st.markdown('<div class="rr-empty">No data</div>', unsafe_allow_html=True)
            else:
                html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                html += '<th>Day</th><th>Signals</th><th>Win</th><th>Loss</th><th>Win Rate</th><th>Avg P&L</th>'
                html += '</tr></thead><tbody>'
                for d_i in range(5):
                    d_data = dow_valid[dow_valid['dow'] == d_i]
                    if d_data.empty:
                        html += f'<tr><td style="font-weight:600">{dow_names[d_i]}</td><td>0</td><td colspan="4" class="c-muted">—</td></tr>'
                        continue
                    dw = len(d_data[d_data[an_rs] == 'WIN'])
                    dl = len(d_data[d_data[an_rs].isin(['LOSS', 'EXPIRED'])])
                    dc = dw + dl
                    dwr = (dw / dc * 100) if dc > 0 else 0
                    davg = d_data['result_pct_f'].mean()
                    wrc_ = 'c-win' if dwr >= 80 else 'c-partial' if dwr >= 50 else 'c-loss' if dc > 0 else 'c-none'
                    ac_ = 'c-win' if davg > 0 else 'c-loss' if davg < 0 else 'c-none'
                    html += f'<tr><td style="font-weight:600">{dow_names[d_i]}</td>'
                    html += f'<td>{len(d_data)}</td>'
                    html += f'<td class="c-win">{dw}</td><td class="c-loss">{dl}</td>'
                    html += f'<td class="{wrc_}">{dwr:.0f}%</td>'
                    html += f'<td class="{ac_}">{davg:+.2f}%</td></tr>'
                html += '</tbody></table></div>'
                st.markdown(html, unsafe_allow_html=True)
                st.markdown('<div class="rr-legend">특정 요일에 승률이 현저히 낮으면 해당 요일 신호를 스킵하는 필터 고려</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

            # ─────────────────────────────────────────
            # 4) SLIPPAGE — 시그널가 vs 진입가 괴리
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">슬리피지 (Signal vs Entry Price)</div>', unsafe_allow_html=True)

            slip = an_cp[(an_cp['signal_price_f'] > 0) & (an_cp['entry_price_f'] > 0)].copy()
            if slip.empty:
                st.markdown('<div class="rr-empty">No slippage data</div>', unsafe_allow_html=True)
            else:
                slip['slip_pct'] = (slip['entry_price_f'] - slip['signal_price_f']) / slip['signal_price_f'] * 100
                html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                html += '<th>Strategy</th><th>Trades</th><th>Avg Slip</th><th>Median</th><th>Max (불리)</th><th>Std Dev</th>'
                html += '</tr></thead><tbody>'
                for s in strategies:
                    ss = slip[slip['strategy'] == s]
                    if ss.empty:
                        html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td>0</td><td colspan="4" class="c-muted">—</td></tr>'
                        continue
                    sl_data = ss['slip_pct']
                    avg_sl = sl_data.mean(); med_sl = sl_data.median()
                    # Max unfavorable = max positive slip (bought higher than signal)
                    max_sl = sl_data.max()
                    std_sl = sl_data.std() if len(sl_data) > 1 else 0.0
                    sc_ = 'var(--green-bright)' if abs(avg_sl) < 1 else 'var(--amber)' if abs(avg_sl) < 3 else 'var(--red-bright)'
                    html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td>{len(ss)}</td>'
                    html += f'<td style="color:{sc_};font-weight:600">{avg_sl:+.2f}%</td>'
                    html += f'<td>{med_sl:+.2f}%</td>'
                    html += f'<td class="c-loss">{max_sl:+.2f}%</td>'
                    html += f'<td class="c-muted">{std_sl:.2f}%</td></tr>'
                html += '</tbody></table></div>'
                st.markdown(html, unsafe_allow_html=True)
                st.markdown('<div class="rr-legend">양수 = 시그널가보다 비싸게 매수 (불리) | 음수 = 시그널가보다 싸게 매수 (유리)</div>', unsafe_allow_html=True)
                # Check if all slippage is 0 (system sets entry=signal by design)
                if abs(slip['slip_pct'].sum()) < 0.01:
                    st.markdown('<div class="rr-legend" style="color:var(--text-muted);font-style:italic">※ 현재 시스템은 시그널가 = 진입가 (애프터마켓 종가 매수). 실거래 시 슬리피지 발생 가능.</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

            # ─────────────────────────────────────────
            # 5) WIN/LOSS STREAK — 연승/연패 분석
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">연승·연패 분석 (Streak)</div>', unsafe_allow_html=True)

            streak_df = an_cp.dropna(subset=['close_date_dt']).sort_values('close_date_dt').copy()
            if len(streak_df) < 2:
                st.markdown('<div class="rr-empty">Not enough data</div>', unsafe_allow_html=True)
            else:
                streak_df['is_win'] = (streak_df[an_rs] == 'WIN').astype(int)
                # Calculate streaks
                def calc_streaks(series):
                    max_w = max_l = cur_w = cur_l = 0
                    all_w = []; all_l = []
                    for v in series:
                        if v == 1:
                            cur_w += 1; cur_l = 0
                            max_w = max(max_w, cur_w)
                        else:
                            cur_l += 1; cur_w = 0
                            max_l = max(max_l, cur_l)
                        if cur_w > 0: all_w.append(cur_w)
                        if cur_l > 0: all_l.append(cur_l)
                    avg_w = sum(all_w) / len(all_w) if all_w else 0
                    avg_l = sum(all_l) / len(all_l) if all_l else 0
                    return max_w, max_l, avg_w, avg_l

                # Overall
                o_mw, o_ml, o_aw, o_al = calc_streaks(streak_df['is_win'])

                html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                html += '<th>Strategy</th><th>Max 연승</th><th>Max 연패</th><th>Avg 연승</th><th>Avg 연패</th>'
                html += '</tr></thead><tbody>'

                # Overall row
                ml_c = 'var(--red-bright)' if o_ml >= 5 else 'var(--amber)' if o_ml >= 3 else 'var(--text-secondary)'
                html += f'<tr style="border-top:2px solid var(--gold-dim)"><td style="color:var(--gold);font-weight:700">TOTAL</td>'
                html += f'<td class="c-win" style="font-weight:700;font-size:1.1em">{o_mw}</td>'
                html += f'<td style="color:{ml_c};font-weight:700;font-size:1.1em">{o_ml}</td>'
                html += f'<td class="c-win">{o_aw:.1f}</td><td class="c-loss">{o_al:.1f}</td></tr>'

                for s in strategies:
                    s_streak = streak_df[streak_df['strategy'] == s]
                    if len(s_streak) < 2:
                        html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td colspan="4" class="c-muted">—</td></tr>'
                        continue
                    s_mw, s_ml, s_aw, s_al = calc_streaks(s_streak['is_win'])
                    sml_c = 'var(--red-bright)' if s_ml >= 5 else 'var(--amber)' if s_ml >= 3 else 'var(--text-secondary)'
                    html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td>'
                    html += f'<td class="c-win">{s_mw}</td><td style="color:{sml_c};font-weight:600">{s_ml}</td>'
                    html += f'<td class="c-win">{s_aw:.1f}</td><td class="c-loss">{s_al:.1f}</td></tr>'
                html += '</tbody></table></div>'
                st.markdown(html, unsafe_allow_html=True)
                st.markdown('<div class="rr-legend">최대 연패가 크면 자금관리(켈리 기준 등) 재검토 필요 — 심리적 한계선 설정 참고</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

            # ─────────────────────────────────────────
            # 6) NEAR-MISS — TP 근접 후 실패 분석
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">아쉬운 실패 (Near-Miss Analysis)</div>', unsafe_allow_html=True)

            non_wins = an_cp[an_cp[an_rs].isin(['LOSS', 'EXPIRED'])].copy()
            if non_wins.empty:
                st.markdown('<div class="rr-empty">No losses/expirations</div>', unsafe_allow_html=True)
            else:
                nm_80 = non_wins[non_wins['max_ach_f'] >= 80]
                nm_50 = non_wins[(non_wins['max_ach_f'] >= 50) & (non_wins['max_ach_f'] < 80)]
                nm_low = non_wins[non_wins['max_ach_f'] < 50]

                total_nw = len(non_wins)
                st.markdown(f'''<div class="rr-stats">
                    <div class="rr-stat"><div class="s-label">Total Loss/Exp</div><div class="s-value" style="color:var(--red-bright)">{total_nw}</div></div>
                    <div class="rr-stat"><div class="s-label">≥80% 도달 후 실패</div><div class="s-value" style="color:var(--amber)">{len(nm_80)}</div></div>
                    <div class="rr-stat"><div class="s-label">50~80% 도달</div><div class="s-value" style="color:var(--text-secondary)">{len(nm_50)}</div></div>
                    <div class="rr-stat"><div class="s-label">&lt;50% (완전 실패)</div><div class="s-value" style="color:var(--red-bright)">{len(nm_low)}</div></div>
                </div>''', unsafe_allow_html=True)

                if not nm_80.empty:
                    nm_rate = len(nm_80) / total_nw * 100
                    st.markdown(f'<div class="rr-legend" style="color:var(--amber)">Near-miss rate: {nm_rate:.0f}% — TP의 80% 이상 도달했지만 실패한 비율이 높으면 TP 하향 조정 검토</div>', unsafe_allow_html=True)

                    # Show the near-miss details
                    html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                    html += '<th>Strat</th><th>Ticker</th><th>Signal</th><th>Result</th><th>Max Ach%</th><th>Result P&L</th><th>Peak</th>'
                    html += '</tr></thead><tbody>'
                    for _, row in nm_80.sort_values('max_ach_f', ascending=False).head(20).iterrows():
                        s_ = safe_str(row.get('strategy')); tk_ = safe_str(row.get('ticker'))
                        sd_ = us_to_kst_short(safe_str(row.get('signal_date'))); res_ = safe_str(row.get(an_rs))
                        ma_ = row['max_ach_f']; rp_ = row['result_pct_f']
                        mx_ = safe_str(row.get('max_price'))
                        html += f'<tr><td>{stag(s_)}</td><td style="font-weight:600">{tk_}</td><td>{sd_}</td>'
                        html += f'<td>{result_badge(res_)}</td>'
                        html += f'<td style="color:var(--amber);font-weight:700">{ma_:.0f}%</td>'
                        html += f'<td class="c-loss">{rp_:+.1f}%</td>'
                        html += f'<td>${mx_}</td></tr>'
                    html += '</tbody></table></div>'
                    st.markdown(html, unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

            # ─────────────────────────────────────────
            # 7) CONCURRENT POSITIONS — 동시 포지션 수
            # ─────────────────────────────────────────
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">동시 포지션 수 (Concurrent Positions)</div>', unsafe_allow_html=True)

            # Build timeline from all positions (open + closed)
            pos_events = []
            for df_src, is_closed in [(an_cp, True)]:
                for _, row in df_src.iterrows():
                    ed = row['entry_date_dt']
                    cd = row['close_date_dt'] if is_closed else pd.NaT
                    if pd.isna(ed):
                        continue
                    pos_events.append({'open': ed, 'close': cd if pd.notna(cd) else pd.Timestamp.now(), 'strategy': safe_str(row.get('strategy'))})
            # Include currently open positions
            if not open_pos.empty and 'entry_date' in open_pos.columns:
                for _, row in open_pos.iterrows():
                    ed = pd.to_datetime(row.get('entry_date', ''), errors='coerce')
                    if pd.isna(ed):
                        continue
                    pos_events.append({'open': ed, 'close': pd.Timestamp.now(), 'strategy': safe_str(row.get('strategy'))})

            if not pos_events:
                st.markdown('<div class="rr-empty">No position data</div>', unsafe_allow_html=True)
            else:
                # Generate daily concurrent count
                all_dates = set()
                for pe in pos_events:
                    dr = pd.date_range(pe['open'], pe['close'], freq='B')  # business days
                    all_dates.update(dr)
                if all_dates:
                    all_dates = sorted(all_dates)
                    daily_counts = []
                    for dt in all_dates:
                        cnt = sum(1 for pe in pos_events if pe['open'] <= dt <= pe['close'])
                        daily_counts.append({'date': dt, 'count': cnt})
                    conc_df = pd.DataFrame(daily_counts).set_index('date')

                    max_conc = conc_df['count'].max()
                    avg_conc = conc_df['count'].mean()
                    max_date = conc_df['count'].idxmax()
                    max_date_s = max_date.strftime('%Y-%m-%d') if hasattr(max_date, 'strftime') else str(max_date)

                    mc_c = 'var(--red-bright)' if max_conc >= 10 else 'var(--amber)' if max_conc >= 5 else 'var(--green-bright)'
                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">Max Concurrent</div><div class="s-value" style="color:{mc_c}">{max_conc}</div></div>
                        <div class="rr-stat"><div class="s-label">Avg Concurrent</div><div class="s-value" style="color:var(--text-secondary)">{avg_conc:.1f}</div></div>
                        <div class="rr-stat"><div class="s-label">Peak Date</div><div class="s-value" style="color:var(--text-secondary);font-size:0.8em">{max_date_s}</div></div>
                    </div>''', unsafe_allow_html=True)

                    st.markdown('<div class="rr-legend">동시 포지션 수 추이 — 자금 배분 및 리스크 노출 관리에 활용</div>', unsafe_allow_html=True)
                    st.area_chart(conc_df, color=['#c9a96e'])
                else:
                    st.markdown('<div class="rr-empty">No date range data</div>', unsafe_allow_html=True)

    # ── 9) Data Quality ──
    with h_dq:
        st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:1em;margin-bottom:16px">데이터 품질 대시보드 (Data Quality)</div>', unsafe_allow_html=True)

        # Load all signal files for quality analysis
        _dq_files = sorted(glob.glob('data/signal_*.csv'))
        _dq_frames = []
        for f in _dq_files:
            try:
                _df = pd.read_csv(f)
                _dq_frames.append(_df)
            except Exception:
                pass
        dq_signals = pd.concat(_dq_frames, ignore_index=True) if _dq_frames else pd.DataFrame()

        if dq_signals.empty:
            st.markdown('<div class="rr-empty">No signal data for quality analysis</div>', unsafe_allow_html=True)
        else:
            # 1) Data completeness — missing fields per strategy
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">필드 완전성 (Field Completeness)</div>', unsafe_allow_html=True)

            critical_fields = ['ticker', 'date', 'price', 'tp_price', 'strategy']
            optional_fields = ['rsi7', 'rsi14', 'intraday', 'ret3d', 'ret1d', 'ret5d', 'consec_down', 'dist_low5', 'vol_avg']

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
            html += '<th>Field</th><th>Present</th><th>Missing</th><th>Fill Rate</th><th>Status</th>'
            html += '</tr></thead><tbody>'
            for fld in critical_fields + optional_fields:
                if fld in dq_signals.columns:
                    present = int(dq_signals[fld].notna().sum())
                    missing = int(dq_signals[fld].isna().sum())
                    total = present + missing
                    rate = (present / total * 100) if total > 0 else 0
                    is_critical = fld in critical_fields
                    sc_ = 'var(--green-bright)' if rate >= 99 else 'var(--amber)' if rate >= 90 else 'var(--red-bright)'
                    status = '✓' if rate >= 99 else '⚠' if rate >= 90 else '✗'
                    fld_style = 'font-weight:600;color:var(--gold)' if is_critical else ''
                    html += f'<tr><td style="{fld_style}">{fld}</td><td>{present}</td><td>{missing}</td>'
                    html += f'<td style="color:{sc_};font-weight:600">{rate:.1f}%</td>'
                    html += f'<td style="color:{sc_}">{status}</td></tr>'
                else:
                    html += f'<tr><td class="c-muted">{fld}</td><td colspan="4" class="c-muted">N/A</td></tr>'
            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # 2) Volume anomaly detection
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">거래량 이상 탐지 (Volume Anomalies)</div>', unsafe_allow_html=True)

            if 'vol_avg' in dq_signals.columns:
                dq_signals['vol_avg_n'] = pd.to_numeric(dq_signals['vol_avg'], errors='coerce')
                vol_data = dq_signals.dropna(subset=['vol_avg_n'])
                if not vol_data.empty:
                    vol_med = vol_data['vol_avg_n'].median()
                    vol_mean = vol_data['vol_avg_n'].mean()
                    vol_std = vol_data['vol_avg_n'].std()
                    low_vol = vol_data[vol_data['vol_avg_n'] < vol_med * 0.3]
                    extreme_vol = vol_data[vol_data['vol_avg_n'] > vol_mean + 3 * vol_std] if vol_std > 0 else pd.DataFrame()

                    lv_c = 'var(--red-bright)' if len(low_vol) > 5 else 'var(--amber)' if len(low_vol) > 0 else 'var(--green-bright)'
                    ev_c = 'var(--amber)' if len(extreme_vol) > 0 else 'var(--green-bright)'
                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">Median Volume</div><div class="s-value" style="color:var(--text-secondary)">{vol_med:,.0f}</div></div>
                        <div class="rr-stat"><div class="s-label">Low Vol Signals</div><div class="s-value" style="color:{lv_c}">{len(low_vol)}</div></div>
                        <div class="rr-stat"><div class="s-label">Extreme Vol</div><div class="s-value" style="color:{ev_c}">{len(extreme_vol)}</div></div>
                    </div>''', unsafe_allow_html=True)

                    if not low_vol.empty:
                        st.markdown('<div class="rr-legend" style="color:var(--amber)">⚠ Low volume signals — 유동성 부족으로 실제 매수/매도 시 슬리피지 위험</div>', unsafe_allow_html=True)
                        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                        html += '<th>Ticker</th><th>Date</th><th>Strategy</th><th>Avg Volume</th><th>Price</th>'
                        html += '</tr></thead><tbody>'
                        for _, r in low_vol.head(15).iterrows():
                            html += f'<tr><td style="font-weight:600">{safe_str(r.get("ticker"))}</td>'
                            html += f'<td>{safe_str(r.get("date"))}</td>'
                            html += f'<td>{stag(safe_str(r.get("strategy")))}</td>'
                            html += f'<td style="color:var(--red-bright)">{r["vol_avg_n"]:,.0f}</td>'
                            html += f'<td>${safe_float(r.get("price",0)):.2f}</td></tr>'
                        html += '</tbody></table></div>'
                        st.markdown(html, unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rr-empty">No volume data available</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">Volume field not present in signals</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # 3) Price anomaly detection
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">가격 이상 탐지 (Price Anomalies)</div>', unsafe_allow_html=True)

            if 'price' in dq_signals.columns:
                dq_signals['price_n'] = pd.to_numeric(dq_signals['price'], errors='coerce')
                pr_data = dq_signals.dropna(subset=['price_n'])
                if not pr_data.empty:
                    penny = pr_data[pr_data['price_n'] < 1.0]
                    zero_pr = pr_data[pr_data['price_n'] <= 0]

                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">Total Signals</div><div class="s-value" style="color:var(--text-secondary)">{len(pr_data)}</div></div>
                        <div class="rr-stat"><div class="s-label">Penny (&lt;$1)</div><div class="s-value" style="color:{"var(--amber)" if len(penny)>0 else "var(--green-bright)"}">{len(penny)}</div></div>
                        <div class="rr-stat"><div class="s-label">Zero/Negative</div><div class="s-value" style="color:{"var(--red-bright)" if len(zero_pr)>0 else "var(--green-bright)"}">{len(zero_pr)}</div></div>
                        <div class="rr-stat"><div class="s-label">Avg Price</div><div class="s-value" style="color:var(--text-secondary)">${pr_data["price_n"].mean():.2f}</div></div>
                    </div>''', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rr-empty">No valid price data</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">Price field not present</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # 4) Data gap detection — signal file date gaps
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">데이터 갭 탐지 (Signal Date Gaps)</div>', unsafe_allow_html=True)

            if 'date' in dq_signals.columns:
                dq_dates = pd.to_datetime(dq_signals['date'], errors='coerce').dropna().dt.normalize().unique()
                dq_dates = sorted(dq_dates)
                if len(dq_dates) >= 2:
                    gaps = []
                    for i in range(1, len(dq_dates)):
                        diff = (dq_dates[i] - dq_dates[i-1]).days
                        if diff > 4:  # More than a long weekend (Fri→Mon = 3)
                            gaps.append({
                                'from': dq_dates[i-1].strftime('%Y-%m-%d'),
                                'to': dq_dates[i].strftime('%Y-%m-%d'),
                                'days': diff
                            })

                    gc = 'var(--green-bright)' if len(gaps) == 0 else 'var(--amber)' if len(gaps) <= 3 else 'var(--red-bright)'
                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">Scan Days</div><div class="s-value" style="color:var(--text-secondary)">{len(dq_dates)}</div></div>
                        <div class="rr-stat"><div class="s-label">Date Range</div><div class="s-value" style="color:var(--text-secondary);font-size:0.7em">{dq_dates[0].strftime("%Y-%m-%d")} ~ {dq_dates[-1].strftime("%Y-%m-%d")}</div></div>
                        <div class="rr-stat"><div class="s-label">Gaps (&gt;4d)</div><div class="s-value" style="color:{gc}">{len(gaps)}</div></div>
                    </div>''', unsafe_allow_html=True)

                    if gaps:
                        st.markdown('<div class="rr-legend" style="color:var(--amber)">⚠ 스캔 누락 기간 — 공휴일이 아닌 경우 GitHub Actions 실패 확인 필요</div>', unsafe_allow_html=True)
                        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                        html += '<th>From</th><th>To</th><th>Gap (days)</th><th>Note</th>'
                        html += '</tr></thead><tbody>'
                        for g in gaps:
                            note = '공휴일 가능' if g['days'] <= 5 else '스캔 누락 의심'
                            nc = 'var(--text-muted)' if g['days'] <= 5 else 'var(--red-bright)'
                            html += f'<tr><td>{g["from"]}</td><td>{g["to"]}</td>'
                            html += f'<td style="color:var(--amber);font-weight:600">{g["days"]}일</td>'
                            html += f'<td style="color:{nc}">{note}</td></tr>'
                        html += '</tbody></table></div>'
                        st.markdown(html, unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="rr-legend" style="color:var(--green-bright)">✓ 모든 거래일에 스캔 데이터 존재 — 데이터 갭 없음</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rr-empty">Not enough dates for gap analysis</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">No date field in signals</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # 5) Duplicate signal detection
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">중복 시그널 탐지 (Duplicate Signals)</div>', unsafe_allow_html=True)

            dup_cols = ['ticker', 'date', 'strategy']
            dup_check_cols = [c for c in dup_cols if c in dq_signals.columns]
            if len(dup_check_cols) == 3:
                dups = dq_signals[dq_signals.duplicated(subset=dup_check_cols, keep=False)]
                dup_count = len(dq_signals[dq_signals.duplicated(subset=dup_check_cols, keep='first')])

                dc = 'var(--green-bright)' if dup_count == 0 else 'var(--amber)' if dup_count <= 3 else 'var(--red-bright)'
                st.markdown(f'''<div class="rr-stats">
                    <div class="rr-stat"><div class="s-label">Total Signals</div><div class="s-value" style="color:var(--text-secondary)">{len(dq_signals)}</div></div>
                    <div class="rr-stat"><div class="s-label">Duplicates</div><div class="s-value" style="color:{dc}">{dup_count}</div></div>
                    <div class="rr-stat"><div class="s-label">Unique</div><div class="s-value" style="color:var(--green-bright)">{len(dq_signals) - dup_count}</div></div>
                </div>''', unsafe_allow_html=True)

                if dup_count > 0:
                    st.markdown('<div class="rr-legend" style="color:var(--amber)">⚠ 동일 날짜/티커/전략의 중복 시그널 발견</div>', unsafe_allow_html=True)
                    html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                    html += '<th>Ticker</th><th>Date</th><th>Strategy</th><th>Price</th>'
                    html += '</tr></thead><tbody>'
                    for _, r in dups.head(20).iterrows():
                        html += f'<tr><td style="font-weight:600">{safe_str(r.get("ticker"))}</td>'
                        html += f'<td>{safe_str(r.get("date"))}</td>'
                        html += f'<td>{stag(safe_str(r.get("strategy")))}</td>'
                        html += f'<td>${safe_float(r.get("price",0)):.2f}</td></tr>'
                    html += '</tbody></table></div>'
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rr-legend" style="color:var(--green-bright)">✓ 중복 시그널 없음</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">Missing columns for duplicate check</div>', unsafe_allow_html=True)

    # ── 10) Portfolio Risk Management ──
    with h_risk:
        st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:1em;margin-bottom:16px">포트폴리오 리스크 관리 (Portfolio Risk)</div>', unsafe_allow_html=True)

        # Combine open + closed for risk analysis
        risk_open = open_pos.copy() if not open_pos.empty else pd.DataFrame()
        risk_closed = closed_pos.copy() if not closed_pos.empty else pd.DataFrame()

        if risk_open.empty and risk_closed.empty:
            st.markdown('<div class="rr-empty">No position data for risk analysis</div>', unsafe_allow_html=True)
        else:
            # ── 10-1) Concentration Analysis — 전략 집중도 ──
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">전략 집중도 (Strategy Concentration)</div>', unsafe_allow_html=True)

            active_pos = risk_open[risk_open['status'].str.upper().isin(['OPEN', 'PENDING'])] if not risk_open.empty and 'status' in risk_open.columns else pd.DataFrame()

            if not active_pos.empty and 'strategy' in active_pos.columns:
                strat_dist = active_pos['strategy'].value_counts()
                total_active = len(active_pos)
                max_strat = strat_dist.index[0] if len(strat_dist) > 0 else '—'
                max_count = int(strat_dist.iloc[0]) if len(strat_dist) > 0 else 0
                max_pct = (max_count / total_active * 100) if total_active > 0 else 0
                conc_c = 'var(--red-bright)' if max_pct > 60 else 'var(--amber)' if max_pct > 40 else 'var(--green-bright)'
                num_strats_used = len(strat_dist)

                st.markdown(f'''<div class="rr-stats">
                    <div class="rr-stat"><div class="s-label">Active Positions</div><div class="s-value" style="color:var(--blue)">{total_active}</div></div>
                    <div class="rr-stat"><div class="s-label">Strategies Used</div><div class="s-value" style="color:var(--text-secondary)">{num_strats_used}/5</div></div>
                    <div class="rr-stat"><div class="s-label">Most Concentrated</div><div class="s-value" style="color:{conc_c}">{stag(max_strat)} {max_pct:.0f}%</div></div>
                </div>''', unsafe_allow_html=True)

                html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                html += '<th>Strategy</th><th>Active</th><th>Share</th><th>Distribution</th>'
                html += '</tr></thead><tbody>'
                for s in strategies:
                    sc_ = int(strat_dist.get(s, 0))
                    sp_ = (sc_ / total_active * 100) if total_active > 0 else 0
                    bar_w = min(sp_, 100)
                    bc_ = 'var(--green-bright)' if sp_ <= 30 else 'var(--amber)' if sp_ <= 50 else 'var(--red-bright)'
                    html += f'<tr><td>{stag(s)} {STRAT_KR.get(s,"")}</td><td>{sc_}</td>'
                    html += f'<td style="font-weight:600">{sp_:.0f}%</td>'
                    html += f'<td><div style="background:var(--bg-elevated);border-radius:4px;overflow:hidden;height:16px">'
                    html += f'<div style="width:{bar_w}%;height:100%;background:{bc_};border-radius:4px"></div></div></td></tr>'
                html += '</tbody></table></div>'
                st.markdown(html, unsafe_allow_html=True)

                if max_pct > 60:
                    st.markdown(f'<div class="rr-legend" style="color:var(--red-bright)">⚠ {max_strat} 전략에 60% 이상 집중 — 다른 전략으로 분산 권장</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">No active positions</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # ── 10-2) Ticker Concentration — 종목 집중도 ──
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">종목 집중도 (Ticker Concentration)</div>', unsafe_allow_html=True)

            if not active_pos.empty and 'ticker' in active_pos.columns:
                tk_dist = active_pos['ticker'].value_counts()
                multi_tk = tk_dist[tk_dist > 1]

                st.markdown(f'''<div class="rr-stats">
                    <div class="rr-stat"><div class="s-label">Unique Tickers</div><div class="s-value" style="color:var(--text-secondary)">{len(tk_dist)}</div></div>
                    <div class="rr-stat"><div class="s-label">Multi-position Tickers</div><div class="s-value" style="color:{"var(--amber)" if len(multi_tk)>0 else "var(--green-bright)"}">{len(multi_tk)}</div></div>
                    <div class="rr-stat"><div class="s-label">Max per Ticker</div><div class="s-value" style="color:{"var(--red-bright)" if (tk_dist.max() if len(tk_dist)>0 else 0)>2 else "var(--text-secondary)"}">{int(tk_dist.max()) if len(tk_dist)>0 else 0}</div></div>
                </div>''', unsafe_allow_html=True)

                if not multi_tk.empty:
                    st.markdown('<div class="rr-legend" style="color:var(--amber)">⚠ 동일 종목 복수 포지션 — 단일 종목 리스크 노출 증가</div>', unsafe_allow_html=True)
                    html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                    html += '<th>Ticker</th><th>Positions</th><th>Strategies</th>'
                    html += '</tr></thead><tbody>'
                    for tk, cnt in multi_tk.items():
                        strats_for_tk = active_pos[active_pos['ticker'] == tk]['strategy'].unique()
                        badges = ' '.join(stag(s) for s in sorted(strats_for_tk))
                        html += f'<tr><td style="font-weight:600">{tk}</td><td style="color:var(--amber);font-weight:600">{cnt}</td><td>{badges}</td></tr>'
                    html += '</tbody></table></div>'
                    st.markdown(html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">No active positions</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # ── 10-3) Concurrent Position Limits — 동시 포지션 한도 분석 ──
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">동시 포지션 한도 분석 (Position Limits)</div>', unsafe_allow_html=True)

            pos_timeline = []
            if not risk_closed.empty and 'entry_date' in risk_closed.columns and 'close_date' in risk_closed.columns:
                for _, r in risk_closed.iterrows():
                    ed = pd.to_datetime(r.get('entry_date', ''), errors='coerce')
                    cd = pd.to_datetime(r.get('close_date', ''), errors='coerce')
                    if pd.notna(ed) and pd.notna(cd):
                        pos_timeline.append({'open': ed, 'close': cd, 'strategy': safe_str(r.get('strategy')), 'ticker': safe_str(r.get('ticker'))})
            if not active_pos.empty and 'entry_date' in active_pos.columns:
                for _, r in active_pos.iterrows():
                    ed = pd.to_datetime(r.get('entry_date', ''), errors='coerce')
                    if pd.notna(ed):
                        pos_timeline.append({'open': ed, 'close': pd.Timestamp.now(), 'strategy': safe_str(r.get('strategy')), 'ticker': safe_str(r.get('ticker'))})

            if pos_timeline:
                all_tl_dates = set()
                for pe in pos_timeline:
                    dr = pd.date_range(pe['open'], pe['close'], freq='B')
                    all_tl_dates.update(dr)
                all_tl_dates = sorted(all_tl_dates)

                if all_tl_dates:
                    daily_conc = []
                    for dt in all_tl_dates:
                        cnt = sum(1 for pe in pos_timeline if pe['open'] <= dt <= pe['close'])
                        daily_conc.append({'date': dt, 'concurrent': cnt})
                    conc_tl = pd.DataFrame(daily_conc)

                    max_c = conc_tl['concurrent'].max()
                    avg_c = conc_tl['concurrent'].mean()
                    days_over_5 = len(conc_tl[conc_tl['concurrent'] > 5])
                    days_over_10 = len(conc_tl[conc_tl['concurrent'] > 10])

                    mc_c2 = 'var(--red-bright)' if max_c > 10 else 'var(--amber)' if max_c > 5 else 'var(--green-bright)'
                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">Peak Concurrent</div><div class="s-value" style="color:{mc_c2}">{max_c}</div></div>
                        <div class="rr-stat"><div class="s-label">Avg Concurrent</div><div class="s-value" style="color:var(--text-secondary)">{avg_c:.1f}</div></div>
                        <div class="rr-stat"><div class="s-label">Days &gt;5 pos</div><div class="s-value" style="color:{"var(--amber)" if days_over_5>0 else "var(--green-bright)"}">{days_over_5}</div></div>
                        <div class="rr-stat"><div class="s-label">Days &gt;10 pos</div><div class="s-value" style="color:{"var(--red-bright)" if days_over_10>0 else "var(--green-bright)"}">{days_over_10}</div></div>
                    </div>''', unsafe_allow_html=True)

                    # Recommend position limit
                    if max_c > 10:
                        st.markdown('<div class="rr-legend" style="color:var(--red-bright)">⚠ 동시 포지션이 10개를 초과하는 날이 있었습니다. 자금 분산을 위해 전략별 최대 포지션 수를 제한하는 것을 권장합니다.</div>', unsafe_allow_html=True)
                    elif max_c > 5:
                        st.markdown('<div class="rr-legend" style="color:var(--amber)">동시 포지션이 5개를 초과하는 날이 있었습니다. 포지션 사이징에 유의하세요.</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="rr-legend" style="color:var(--green-bright)">✓ 동시 포지션 수가 양호합니다.</div>', unsafe_allow_html=True)

                    # Chart
                    chart_conc = conc_tl.set_index('date')
                    st.area_chart(chart_conc, color=['#c9a96e'])
            else:
                st.markdown('<div class="rr-empty">No timeline data for position limits</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # ── 10-4) Risk Score per Active Position ──
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">활성 포지션 리스크 스코어 (Active Position Risk)</div>', unsafe_allow_html=True)

            if not active_pos.empty and all(c in active_pos.columns for c in ['entry_price', 'current_price', 'days_held', 'max_hold', 'strategy']):
                risk_rows = []
                for _, r in active_pos.iterrows():
                    ep_ = pd.to_numeric(r.get('entry_price', 0), errors='coerce') or 0
                    cp_ = pd.to_numeric(r.get('current_price', 0), errors='coerce') or 0
                    dh_ = pd.to_numeric(r.get('days_held', 0), errors='coerce') or 0
                    mh_ = pd.to_numeric(r.get('max_hold', 5), errors='coerce') or 5
                    sl_ = pd.to_numeric(r.get('sl_price', 0), errors='coerce') or 0

                    if ep_ <= 0:
                        continue

                    pnl_pct = (cp_ - ep_) / ep_ * 100 if cp_ > 0 else 0

                    # Risk components (0-100 scale)
                    # SL distance: closer to SL = higher risk (40pts max)
                    if sl_ > 0 and cp_ > 0:
                        sl_dist_pct = (cp_ - sl_) / cp_ * 100
                        sl_risk = max(0, min(40, 40 * (1 - sl_dist_pct / 20)))
                    else:
                        sl_risk = 10  # No SL = moderate risk

                    # Time pressure: closer to expiry = higher risk (30pts max)
                    time_ratio = dh_ / mh_ if mh_ > 0 else 0
                    time_risk = min(30, 30 * time_ratio)

                    # P&L risk: negative P&L = higher risk (30pts max)
                    pnl_risk = max(0, min(30, -pnl_pct * 3)) if pnl_pct < 0 else 0

                    total_risk = sl_risk + time_risk + pnl_risk

                    risk_rows.append({
                        'ticker': safe_str(r.get('ticker')),
                        'strategy': safe_str(r.get('strategy')),
                        'entry_price': ep_,
                        'current_price': cp_,
                        'pnl_pct': pnl_pct,
                        'days_held': int(dh_),
                        'max_hold': int(mh_),
                        'risk_score': total_risk,
                        'sl_risk': sl_risk,
                        'time_risk': time_risk,
                        'pnl_risk': pnl_risk,
                    })

                if risk_rows:
                    risk_df = pd.DataFrame(risk_rows).sort_values('risk_score', ascending=False)
                    high_risk = risk_df[risk_df['risk_score'] >= 60]
                    med_risk = risk_df[(risk_df['risk_score'] >= 30) & (risk_df['risk_score'] < 60)]

                    hr_c = 'var(--red-bright)' if len(high_risk) > 0 else 'var(--green-bright)'
                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">High Risk (≥60)</div><div class="s-value" style="color:{hr_c}">{len(high_risk)}</div></div>
                        <div class="rr-stat"><div class="s-label">Medium Risk</div><div class="s-value" style="color:var(--amber)">{len(med_risk)}</div></div>
                        <div class="rr-stat"><div class="s-label">Low Risk</div><div class="s-value" style="color:var(--green-bright)">{len(risk_df) - len(high_risk) - len(med_risk)}</div></div>
                        <div class="rr-stat"><div class="s-label">Avg Score</div><div class="s-value" style="color:var(--text-secondary)">{risk_df["risk_score"].mean():.0f}</div></div>
                    </div>''', unsafe_allow_html=True)

                    html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                    html += '<th>Ticker</th><th>Strategy</th><th>P&L</th><th>Day</th>'
                    html += '<th>SL Risk</th><th>Time Risk</th><th>P&L Risk</th><th>Total</th>'
                    html += '</tr></thead><tbody>'
                    for _, rr in risk_df.iterrows():
                        rs_ = rr['risk_score']
                        rc_ = 'var(--red-bright)' if rs_ >= 60 else 'var(--amber)' if rs_ >= 30 else 'var(--green-bright)'
                        pc_ = 'c-win' if rr['pnl_pct'] > 0 else 'c-loss' if rr['pnl_pct'] < 0 else 'c-muted'
                        html += f'<tr><td style="font-weight:600">{rr["ticker"]}</td>'
                        html += f'<td>{stag(rr["strategy"])}</td>'
                        html += f'<td class="{pc_}">{rr["pnl_pct"]:+.1f}%</td>'
                        html += f'<td>{rr["days_held"]}/{rr["max_hold"]}</td>'
                        html += f'<td>{rr["sl_risk"]:.0f}</td><td>{rr["time_risk"]:.0f}</td><td>{rr["pnl_risk"]:.0f}</td>'
                        html += f'<td style="color:{rc_};font-weight:700;font-size:1.1em">{rs_:.0f}</td></tr>'
                    html += '</tbody></table></div>'
                    st.markdown(html, unsafe_allow_html=True)
                    st.markdown('<div class="rr-legend">Risk Score = SL거리(40) + 시간압박(30) + 손실(30) — 60 이상이면 주의 필요</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rr-empty">No active positions with valid data</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">Insufficient data for risk scoring</div>', unsafe_allow_html=True)

            st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)

            # ── 10-5) Historical Drawdown Recovery ──
            st.markdown('<div class="rr-legend" style="color:var(--gold);font-size:0.95em;margin-bottom:12px">드로다운 회복 분석 (Drawdown Recovery)</div>', unsafe_allow_html=True)

            if not risk_closed.empty and 'result_pct' in risk_closed.columns and 'close_date' in risk_closed.columns:
                dd_df = risk_closed[['close_date', 'result_pct', 'strategy']].copy()
                dd_df['result_pct'] = pd.to_numeric(dd_df['result_pct'], errors='coerce')
                dd_df['close_date'] = pd.to_datetime(dd_df['close_date'], errors='coerce')
                dd_df = dd_df.dropna().sort_values('close_date')

                if len(dd_df) >= 3:
                    dd_df['cum_pnl'] = dd_df['result_pct'].cumsum()
                    dd_df['peak'] = dd_df['cum_pnl'].cummax()
                    dd_df['drawdown'] = dd_df['cum_pnl'] - dd_df['peak']

                    # Find drawdown periods
                    in_dd = False
                    dd_periods = []
                    dd_start = None
                    dd_peak_val = 0

                    for _, r in dd_df.iterrows():
                        if r['drawdown'] < -1 and not in_dd:
                            in_dd = True
                            dd_start = r['close_date']
                            dd_peak_val = r['peak']
                        elif r['drawdown'] >= 0 and in_dd:
                            in_dd = False
                            dd_periods.append({
                                'start': dd_start,
                                'end': r['close_date'],
                                'depth': dd_df[(dd_df['close_date'] >= dd_start) & (dd_df['close_date'] <= r['close_date'])]['drawdown'].min(),
                                'duration': (r['close_date'] - dd_start).days
                            })

                    # Still in drawdown
                    if in_dd:
                        dd_periods.append({
                            'start': dd_start,
                            'end': dd_df['close_date'].max(),
                            'depth': dd_df[dd_df['close_date'] >= dd_start]['drawdown'].min(),
                            'duration': (dd_df['close_date'].max() - dd_start).days,
                        })

                    max_dd = dd_df['drawdown'].min()
                    cur_dd = dd_df['drawdown'].iloc[-1]
                    mdc = 'var(--red-bright)' if max_dd < -10 else 'var(--amber)' if max_dd < -5 else 'var(--green-bright)'
                    cdc = 'var(--red-bright)' if cur_dd < -5 else 'var(--amber)' if cur_dd < 0 else 'var(--green-bright)'

                    st.markdown(f'''<div class="rr-stats">
                        <div class="rr-stat"><div class="s-label">Max Drawdown</div><div class="s-value" style="color:{mdc}">{max_dd:.1f}%</div></div>
                        <div class="rr-stat"><div class="s-label">Current DD</div><div class="s-value" style="color:{cdc}">{cur_dd:.1f}%</div></div>
                        <div class="rr-stat"><div class="s-label">DD Periods</div><div class="s-value" style="color:var(--text-secondary)">{len(dd_periods)}</div></div>
                        <div class="rr-stat"><div class="s-label">Longest DD</div><div class="s-value" style="color:var(--text-secondary)">{max(p["duration"] for p in dd_periods) if dd_periods else 0}일</div></div>
                    </div>''', unsafe_allow_html=True)

                    if dd_periods:
                        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
                        html += '<th>Start</th><th>End</th><th>Depth</th><th>Duration</th><th>Status</th>'
                        html += '</tr></thead><tbody>'
                        for p in sorted(dd_periods, key=lambda x: x['depth']):
                            ps = p['start'].strftime('%Y-%m-%d') if hasattr(p['start'], 'strftime') else str(p['start'])
                            pe = p['end'].strftime('%Y-%m-%d') if hasattr(p['end'], 'strftime') else str(p['end'])
                            recovered = p['end'] < dd_df['close_date'].max() or cur_dd >= 0
                            status = '<span class="c-win">Recovered</span>' if recovered else '<span class="c-loss">Active</span>'
                            html += f'<tr><td>{ps}</td><td>{pe}</td>'
                            html += f'<td style="color:var(--red-bright);font-weight:600">{p["depth"]:.1f}%</td>'
                            html += f'<td>{p["duration"]}일</td><td>{status}</td></tr>'
                        html += '</tbody></table></div>'
                        st.markdown(html, unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rr-empty">Not enough closed positions</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="rr-empty">No closed position data</div>', unsafe_allow_html=True)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('''<div style="text-align:center;padding:16px 0">
        <div style="font-family:var(--font-display);font-size:1.2em;color:var(--text-primary);letter-spacing:0.1em">SURGE</div>
        <div style="font-family:var(--font-display);font-size:0.9em;color:var(--gold);letter-spacing:0.15em">SCANNER</div>
    </div>''', unsafe_allow_html=True)
    st.divider()

    st.markdown("""
| Name | WR | Hold |
|------|-----|------|
| A_5%5일 | 90.1% | 5d |
| B_5%5일 | 86.9% | 5d |
| C_10%30일 | 91.0% | 30d |
| D_15%10일 | 90.3% | 10d |
| E_20%30일 | 97.7% | 30d |
| F_50%20일 | 87.5% | 20d |
| G_40%20일 | 90.0% | 20d |
| H_40%20일 | 90.0% | 20d |
| I_10%5일 | 100.0% | 5d |
| J_10%5일 | 90.0% | 5d |

---
Auto-scan via GitHub Actions
    """)
    st.divider()
    if tracker_info:
        st.markdown(f"**Last tracked:** {tracker_info.get('last_tracked', 'N/A')}")
        st.markdown(f"Active: {tracker_info.get('open_count',0)} | Pending: {tracker_info.get('pending_count',0)} | Closed: {tracker_info.get('closed_count',0)}")
    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.markdown("**Auto Refresh**")
    ar = st.toggle("Enable", value=False, key='ar')
    ri = st.select_slider("Interval", options=[1,2,3,5,10,15,30], value=5, key='ri')
    st.caption(f"{'Active' if ar else 'Paused'} — {ri}min")
    if ar:
        import streamlit.components.v1 as components
        components.html(f"""<script>setTimeout(function(){{ window.parent.location.reload(); }}, {ri*60*1000});</script>""", height=0)
