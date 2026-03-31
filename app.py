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
from datetime import datetime, timedelta, timezone
from collections import defaultdict

KST = timezone(timedelta(hours=9))

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
.stag-A { color: var(--green-bright); border-color: var(--green); background: #4a9e7d12; }
.stag-B { color: var(--blue); border-color: #4a7a9e66; background: #4a7a9e0a; }
.stag-C { color: var(--amber); border-color: #b8954a66; background: #b8954a0a; }
.stag-D { color: var(--red-bright); border-color: var(--red); background: #9e4a5a0a; }
.stag-E { color: #9a7abf; border-color: #7a5aaf66; background: #7a5aaf0a; }

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
.rr-strat-info.si-A::before { background: var(--green); }
.rr-strat-info.si-B::before { background: var(--blue); }
.rr-strat-info.si-C::before { background: var(--amber); }
.rr-strat-info.si-D::before { background: var(--red); }
.rr-strat-info.si-E::before { background: #7a5aaf; }
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
    import glob
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
STRAT_TAB = {'A': '5%5일a', 'B': '15%10일', 'C': '5%5일b', 'D': '20%30일', 'E': '10%30일'}
STRAT_NAMES = {'A': '5%5일a · 급락반등', 'B': '15%10일 · 고수익', 'C': '5%5일b · 과매도', 'D': '20%30일 · 초저가', 'E': '10%30일 · 속반등'}
STRAT_KR = {'A': '급락반등', 'B': '고수익', 'C': '과매도', 'D': '초저가', 'E': '속반등'}
STRAT_TP = {'A': '+5%', 'B': '+15%', 'C': '+5%', 'D': '+20%', 'E': '+10%'}
STRAT_TP_NUM = {'A': 5, 'B': 15, 'C': 5, 'D': 20, 'E': 10}
STRAT_BT_WR = {'A': '90.1%', 'B': '90.3%', 'C': '86.9%', 'D': '97.7%', 'E': '91.0%'}
STRAT_MAX_HOLD = {'A': 5, 'B': 10, 'C': 5, 'D': 30, 'E': 30}

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
    return f'<span class="stag stag-{s}">{STRAT_TAB.get(s, s)}</span>'

def cell_html(ach, det, loss=0, prog=0):
    if det == 0: return '<span class="c-none">—</span>'
    if ach == det: return f'<span class="c-win">{ach}/{det}</span>'
    if prog > 0 and loss == 0 and ach == 0:
        return f'<span class="c-pending">{det}</span>'
    if prog > 0:
        return f'<span class="c-partial">{ach}/{det-prog}</span> <span class="c-pending">+{prog}</span>'
    if ach == 0: return f'<span class="c-loss">0/{det}</span>'
    return f'<span class="c-partial">{ach}/{det}</span>'

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
    m = {'WIN': ('st-win', 'WIN'), 'LOSS': ('st-loss', 'LOSS'), 'EXPIRED': ('st-expired', 'EXP'),
         'OPEN': ('st-open', 'OPEN'), 'PENDING': ('st-pending', 'WAIT')}
    cls, txt = m.get(r, ('c-muted', r))
    return f'<span class="{cls}">{txt}</span>'

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
    counts = ' / '.join(f'{k}:{scan_info.get(f"strategy_{k.lower()}_count", 0)}' for k in 'ABCDE')
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

tab_today, tab_a, tab_b, tab_c, tab_d, tab_e, tab_history = st.tabs([
    _today_label, "5%5일a", "15%10일", "5%5일b", "20%30일", "10%30일", "Performance"
])

# ─── Strategy Tab Builder ─────────────────────────────────────────────────────
STRAT_INFO = {
    'A': {
        'title': '5%5일a — 급락반등',
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
    'B': {
        'title': '15%10일 — 고수익',
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
    'C': {
        'title': '5%5일b — 과매도',
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
    'D': {
        'title': '20%30일 — 초저가',
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
    'E': {
        'title': '10%30일 — 속반등',
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
with tab_today:
    if today_signals.empty:
        st.markdown('<div class="rr-empty">오늘 감지된 신호가 없습니다</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'''<div class="rr-legend" style="color:var(--gold);font-size:0.9em;margin-bottom:16px">
            Today — <span style="font-weight:700;font-size:1.1em">{len(today_signals)}</span> signal{"s" if len(today_signals)>1 else ""} detected
        </div>''', unsafe_allow_html=True)

        # Build the today table grouped by strategy
        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
        html += '<th>Strategy</th><th>Target</th><th>Hold</th><th>Ticker</th>'
        html += '<th>Price</th><th>TP Price</th><th>Backtest</th>'
        html += '</tr></thead><tbody>'

        for s_key in ['A', 'B', 'C', 'D', 'E']:
            if 'strategy' not in today_signals.columns:
                continue
            s_sig = today_signals[today_signals['strategy'] == s_key]
            if s_sig.empty:
                continue
            for idx, row in s_sig.iterrows():
                tk = safe_str(row.get('ticker'))
                pr = safe_float(row.get('price', 0))
                tp = safe_float(row.get('tp_price', 0))
                pr_h = f'${pr:.2f}' if pr > 0 else '—'
                tp_h = f'<span style="color:var(--gold);font-weight:600">${tp:.2f}</span>' if tp > 0 else '—'
                tab_name = STRAT_TAB.get(s_key, s_key)

                html += f'<tr>'
                html += f'<td>{stag(s_key)} <span style="color:var(--text-muted);font-size:0.85em">{STRAT_KR.get(s_key,"")}</span></td>'
                html += f'<td style="color:var(--gold);font-weight:700;font-size:1.05em">{STRAT_TP.get(s_key,"")}</td>'
                html += f'<td style="color:var(--text-secondary)">{STRAT_MAX_HOLD.get(s_key,"")}일</td>'
                html += f'<td style="font-weight:700;color:var(--text-primary);font-size:1.05em">{tk}</td>'
                html += f'<td>{pr_h}</td>'
                html += f'<td>{tp_h}</td>'
                html += f'<td style="color:var(--text-muted)">{STRAT_BT_WR.get(s_key,"—")}</td>'
                html += f'</tr>'

        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)

        # Strategy breakdown summary
        st.markdown('<div class="rr-divider" style="margin:24px auto"></div>', unsafe_allow_html=True)
        strat_counts = today_signals.groupby('strategy').size() if 'strategy' in today_signals.columns else pd.Series(dtype=int)
        cards_html = '<div class="rr-cards" style="grid-template-columns:repeat(auto-fit,minmax(120px,1fr))">'
        for s_key in ['A', 'B', 'C', 'D', 'E']:
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

for k, t in [('A', tab_a), ('B', tab_b), ('C', tab_c), ('D', tab_d), ('E', tab_e)]:
    render_strategy_tab(k, t)

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
        all_records['date_str'] = all_records['signal_date'].dt.strftime('%m/%d')
        all_records['month_str'] = all_records['signal_date'].dt.strftime('%Y-%m')

    strategies = ['A', 'B', 'C', 'D', 'E']
    rs_col = 'result_status' if (not closed_pos.empty and 'result_status' in closed_pos.columns) else 'status'

    # Aggregate
    total_det = len(all_records)
    total_closed = len(closed_pos) if not closed_pos.empty else 0
    total_win = len(closed_pos[closed_pos[rs_col] == 'WIN']) if total_closed > 0 else 0
    total_loss = len(closed_pos[closed_pos[rs_col] == 'LOSS']) if total_closed > 0 else 0
    total_exp = len(closed_pos[closed_pos[rs_col] == 'EXPIRED']) if total_closed > 0 else 0
    total_open = len(open_pos[open_pos['status'].isin(['OPEN', 'PENDING'])]) if not open_pos.empty and 'status' in open_pos.columns else 0
    wr = (total_win / total_closed * 100) if total_closed > 0 else 0

    # Summary cards
    wr_color = 'var(--green-bright)' if wr >= 80 else 'var(--amber)' if wr >= 50 else 'var(--red-bright)'
    st.markdown(f'''<div class="rr-cards">
        <div class="rr-card"><div class="val" style="color:var(--text-secondary)">{total_det}</div><div class="lbl">Detected</div></div>
        <div class="rr-card"><div class="val" style="color:var(--blue)">{total_open}</div><div class="lbl">Active</div></div>
        <div class="rr-card"><div class="val" style="color:var(--green-bright)">{total_win}</div><div class="lbl">Wins</div></div>
        <div class="rr-card"><div class="val" style="color:var(--red-bright)">{total_loss + total_exp}</div><div class="lbl">Loss / Exp</div></div>
        <div class="rr-card accent"><div class="val" style="color:var(--gold)">{wr:.1f}%</div><div class="lbl">Win Rate</div></div>
    </div>''', unsafe_allow_html=True)

    # Sub tabs
    h_daily, h_monthly, h_winrate, h_active, h_closed_detail, h_ticker, h_pnl = st.tabs([
        "Daily", "Monthly", "Strategy", "Active", "Closed", "By Ticker", "P&L"
    ])

    # ── 1) Daily Matrix ──
    with h_daily:
        if all_records.empty:
            st.markdown('<div class="rr-empty">No data yet — run scanner first</div>', unsafe_allow_html=True)
        else:
            cutoff = all_records['signal_date'].max() - pd.Timedelta(days=30)
            recent = all_records[all_records['signal_date'] >= cutoff].copy()
            date_order = sorted(recent['date_str'].unique(), key=lambda x: pd.to_datetime(x, format='%m/%d'))

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
            nw = len(s_cl[s_cl[rs_col]=='WIN']) if nc>0 else 0
            nl = len(s_cl[s_cl[rs_col]=='LOSS']) if nc>0 else 0
            ne = len(s_cl[s_cl[rs_col]=='EXPIRED']) if nc>0 else 0
            na = len(s_op)
            sw = (nw/nc*100) if nc>0 else 0
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
            html += '<th>Strat</th><th>Ticker</th><th>Signal</th><th>Entry</th>'
            html += '<th>Price</th><th>Current</th><th>P&L</th>'
            html += '<th>TP</th><th>Peak</th><th>Peak Date</th>'
            html += '<th>Achievement</th><th>Remaining</th>'
            html += '<th>Days</th><th>Status</th>'
            html += '</tr></thead><tbody>'

            for _, row in active.iterrows():
                s=safe_str(row.get('strategy')); tk=safe_str(row.get('ticker'))
                sig_dt=safe_str(row.get('signal_date')); status=safe_str(row.get('status'))
                is_p = (status=='PENDING')
                sp = safe_float(safe_str(row.get('signal_price')))
                ep_raw=safe_str(row.get('entry_price')); cp_raw=safe_str(row.get('current_price'))
                tp_raw=safe_str(row.get('tp_price')); mx_raw=safe_str(row.get('max_price'))
                mx_dt=safe_str(row.get('max_price_date'))
                ach_raw=safe_str(row.get('achievement_pct')); chg_raw=safe_str(row.get('change_pct'))
                dh=safe_str(row.get('days_held')); mh_raw=safe_str(row.get('max_hold'))
                mh = mh_raw if mh_raw!='—' else str(STRAT_MAX_HOLD.get(s,''))

                if is_p:
                    ep_h=f'<span class="c-muted">${sp:.2f}</span>' if sp>0 else '<span class="c-muted">—</span>'
                    cp_h=ep_h; chg_h='<span class="c-muted">WAIT</span>'
                    mx_h='<span class="c-muted">—</span>'; mx_dt_h='<span class="c-muted">—</span>'
                    ach_h='<span class="c-muted">—</span>'
                    tp_f=safe_float(tp_raw)
                    rem_h = f'<span class="c-muted">~{STRAT_TP.get(s,"")}</span>'
                    days_h=f'<span class="c-muted">0/{mh}</span>'
                    ent_dt_h='<span class="c-muted">WAIT</span>'
                else:
                    ent_dt_h=safe_str(row.get('entry_date'))
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
                    days_h=f'{dh}/{mh}' if dh!='—' else '—'

                st_cls = 'st-open' if status=='OPEN' else 'st-pending'
                st_txt = 'OPEN' if status=='OPEN' else 'WAIT'

                html += f'<tr><td>{stag(s)}</td><td style="font-weight:600;color:var(--text-primary)">{tk}</td>'
                html += f'<td>{sig_dt}</td><td>{ent_dt_h}</td><td>{ep_h}</td><td>{cp_h}</td><td>{chg_h}</td>'
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
                        st.markdown(f"**Signal**\n- Strategy: {STRAT_TAB.get(s,s)} ({STRAT_KR.get(s,'')})\n- Date: {safe_str(row.get('signal_date'))}\n- Price: ${sp:.2f}" if sp>0 else f"**Signal**\n- Strategy: {STRAT_TAB.get(s,s)}\n- Date: {safe_str(row.get('signal_date'))}\n- Price: —")
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
                sig_dt=safe_str(row.get('signal_date')); ent_dt=safe_str(row.get('entry_date'))
                ent_pr=safe_str(row.get('entry_price')); result=safe_str(row.get(rs_col,'status'))
                close_dt=safe_str(row.get('close_date')); close_pr=safe_str(row.get('close_price'))
                rp_raw=safe_str(row.get('result_pct')); tp_hit=safe_str(row.get('tp_hit_date'))
                mx=safe_str(row.get('max_price')); mx_dt=safe_str(row.get('max_price_date'))
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
                    tp_hit=p['tp_hit_date']; mx=p['max_price']; mx_dt=p['max_price_date']; ma=p['max_ach']
                    ep=p['entry_price']; sig_pr=p.get('signal_price','—'); sp_f=safe_float(sig_pr)
                    is_pend=(res=='PENDING')

                    if is_pend:
                        ed_h='<span class="c-muted">WAIT</span>'
                        ep_h=f'<span class="c-muted">${sp_f:.2f}</span>' if sp_f>0 else '<span class="c-muted">—</span>'
                        rp_h='<span class="c-muted">—</span>'; tp_h='<span class="c-muted">—</span>'
                        mx_h='<span class="c-muted">—</span>'; mxd_h='<span class="c-muted">—</span>'
                        ach_h='<span class="c-muted">—</span>'
                    else:
                        ed_h=p['entry_date'] if p['entry_date']!='—' else '<span class="c-muted">—</span>'
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

                    html += f'<tr><td>{stag(s_)}</td><td>{p["signal_date"]}</td><td>{ed_h}</td><td>{ep_h}</td>'
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
                sc={'A':'#4a9e7d','B':'#4a7a9e','C':'#b8954a','D':'#9e4a5a','E':'#7a5aaf'}

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
                    pk=cs.cummax(); dd=cs-pk; mv=dd.min()
                    if mv==0: return 0,None,None,pd.Series(0,index=cs.index)
                    me=dd.idxmin(); ms=cs.loc[:me].idxmax()
                    return mv,ms,me,dd

                tc=chart_data['Total'] if 'Total' in chart_data.columns else pd.Series(dtype=float)
                tm,tms,tme,tdd = calc_mdd(tc) if not tc.empty else (0,None,None,pd.Series(dtype=float))

                smdd={}
                for s in strats_data:
                    if s in chart_data.columns:
                        sv,ss,se,sd=calc_mdd(chart_data[s])
                        smdd[s]={'mdd':sv,'start':ss,'end':se,'dd':sd}

                tp=pnl_df['result_pct'].sum(); ap=pnl_df['result_pct'].mean()
                nt=len(pnl_df); nw=len(pnl_df[pnl_df['result_pct']>0])
                tw=(nw/nt*100) if nt>0 else 0
                tc_='var(--green-bright)' if tp>0 else 'var(--red-bright)' if tp<0 else 'var(--text-muted)'
                mc_='var(--red-bright)' if tm<-5 else 'var(--amber)' if tm<0 else 'var(--text-muted)'
                wc_='var(--green-bright)' if tw>=80 else 'var(--amber)' if tw>0 else 'var(--text-muted)'

                st.markdown(f'''<div class="rr-stats">
                    <div class="rr-stat"><div class="s-label">Cumulative P&L</div><div class="s-value" style="color:{tc_}">{tp:+.1f}%</div></div>
                    <div class="rr-stat"><div class="s-label">Avg per Trade</div><div class="s-value" style="color:{tc_}">{ap:+.2f}%</div></div>
                    <div class="rr-stat"><div class="s-label">Trades</div><div class="s-value" style="color:var(--text-secondary)">{nt}</div></div>
                    <div class="rr-stat"><div class="s-label">Win Rate</div><div class="s-value" style="color:{wc_}">{tw:.0f}%</div></div>
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
| 5%5일a | 90.1% | 5d |
| 15%10일 | 90.3% | 10d |
| 5%5일b | 86.9% | 5d |
| 20%30일 | 97.7% | 30d |
| 10%30일 | 91.0% | 30d |

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
