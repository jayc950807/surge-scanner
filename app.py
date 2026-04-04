"""
SURGE SCANNER DASHBOARD
US Stock Surge Scanner - Luxury Dashboard Interface with Advanced Analytics
"If Rolls-Royce made a trading app"
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import traceback
import numpy as np

# Try to import KST from shared_config, fallback to local definition
try:
    from shared_config import KST
except ImportError:
    KST = timezone(timedelta(hours=9))

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

STRAT_TAB = {
    'A': '5%5일a',
    'B': '15%10일',
    'C': '5%5일b',
    'D': '20%30일',
    'E': '10%30일'
}

STRAT_NAMES = {
    'A': '급락반등',
    'B': '고수익',
    'C': '과매도',
    'D': '초저가',
    'E': '속반등'
}

STRAT_KR = {
    'A': '급락반등',
    'B': '고수익',
    'C': '과매도',
    'D': '초저가',
    'E': '속반등'
}

STRAT_TP_NUM = {
    'A': 5,
    'B': 15,
    'C': 5,
    'D': 20,
    'E': 10
}

STRAT_BT_WR = {
    'A': '90.1%',
    'B': '90.3%',
    'C': '86.9%',
    'D': '97.7%',
    'E': '91.0%'
}

STRAT_TP = {
    'A': '+5%',
    'B': '+15%',
    'C': '+5%',
    'D': '+20%',
    'E': '+10%'
}

STRAT_MAX_HOLD = {
    'A': 5,
    'B': 10,
    'C': 5,
    'D': 30,
    'E': 30
}

STRAT_COLORS = {
    'A': '#4a9e7d',
    'B': '#4a7a9e',
    'C': '#b8954a',
    'D': '#9e4a5a',
    'E': '#7a5aaf'
}

COLOR_PALETTE = {
    'bg_deep': '#080b12',
    'bg_surface': '#0d1117',
    'bg_card': '#131920',
    'bg_elevated': '#1a2230',
    'bg_hover': '#1e2a3a',
    'border_subtle': '#1c2636',
    'gold': '#c9a96e',
    'gold_dim': '#9a7d4e',
    'gold_glow': '#c9a96e18',
    'text_primary': '#e8e4de',
    'text_secondary': '#8a9ab5',
    'text_muted': '#4d5b72',
    'green': '#4a9e7d',
    'red': '#9e4a5a',
    'amber': '#b8954a',
}

DATA_DIR = Path('data')

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def safe_float(val, default=0.0):
    """Safely convert value to float"""
    if val is None or val == '' or pd.isna(val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_str(val, default=''):
    """Safely convert value to string"""
    if val is None or pd.isna(val):
        return default
    return str(val).strip()


def format_currency(val, decimals=2):
    """Format value as USD currency"""
    val = safe_float(val)
    return f"${val:,.{decimals}f}"


def format_percent(val, decimals=1):
    """Format value as percentage"""
    val = safe_float(val)
    sign = '+' if val >= 0 else ''
    return f"{sign}{val:.{decimals}f}%"


def get_strategy_color(strategy):
    """Get color hex for strategy"""
    return STRAT_COLORS.get(strategy, COLOR_PALETTE['text_secondary'])


def get_strategy_name(strategy):
    """Get display name for strategy"""
    return STRAT_NAMES.get(strategy, strategy)


def stag(s):
    """Strategy tag HTML"""
    return f'<span class="stag stag-{s}">{STRAT_TAB[s]}</span>'


def cell_html(ach, det, loss, prog):
    """Matrix cell renderer"""
    if loss:
        cls = 'c-loss'
        val = f"L {det}"
    elif ach >= 100:
        cls = 'c-win'
        val = f"W {det}"
    elif ach >= 50:
        cls = 'c-partial'
        val = f"P {det}"
    elif prog:
        cls = 'c-pending'
        val = f"{ach:.0f}%"
    else:
        cls = 'c-none'
        val = '—'
    return f'<div class="{cls}">{val}</div>'


def progress_bar(val, color='#c9a96e'):
    """Progress bar HTML"""
    pct = max(0, min(100, safe_float(val)))
    return f'''<div class="rr-prog">
        <div class="rr-prog-track">
            <div class="rr-prog-fill" style="width:{pct}%; background-color:{color};"></div>
        </div>
        <div class="rr-prog-text">{pct:.0f}%</div>
    </div>'''


def chg_html(val_raw):
    """Colored +/- percentage"""
    val = safe_float(val_raw)
    sign = '+' if val >= 0 else ''
    color = COLOR_PALETTE['green'] if val >= 0 else COLOR_PALETTE['red']
    return f'<span style="color:{color}">{sign}{val:.2f}%</span>'


def result_badge(r):
    """WIN/LOSS/EXPIRED/OPEN/PENDING badge"""
    r = safe_str(r).upper()
    if r == 'WIN':
        return f'<span class="st-win">WIN</span>'
    elif r == 'LOSS':
        return f'<span class="st-loss">LOSS</span>'
    elif r == 'EXPIRED':
        return f'<span class="st-expired">EXPIRED</span>'
    elif r == 'OPEN':
        return f'<span class="st-open">OPEN</span>'
    else:
        return f'<span class="st-pending">PENDING</span>'


def calc_risk_score(row):
    """
    Calculate risk score for a position. Higher = more dangerous = shows first.
    Scale: 0-100+ points
    """
    score = 0.0

    try:
        entry = safe_float(row.get('entry_price', 0))
        current = safe_float(row.get('current_price', 0))
        sl = safe_float(row.get('sl_price', 0))
        tp = safe_float(row.get('tp_price', 0))
        days = safe_float(row.get('days_held', 0))
        max_hold = safe_float(row.get('max_hold', 0))
        change_pct = safe_float(row.get('change_pct', 0))

        # Factor 1: Closeness to stop loss (0-40 points)
        if sl > 0 and entry > 0 and current > sl:
            sl_distance_pct = ((current - sl) / entry) * 100
            score += max(0, 40 - (sl_distance_pct * 20))

        # Factor 2: Time pressure (0-30 points)
        if max_hold > 0 and days >= 0:
            time_used_ratio = days / max_hold
            score += min(30, time_used_ratio * 30)

        # Factor 3: Negative performance (0-30 points)
        if change_pct < 0:
            score += min(30, abs(change_pct) * 1.5)

    except Exception:
        pass

    return score


def calc_mdd(series):
    """Calculate Maximum Drawdown and period"""
    if len(series) == 0:
        return 0, None, None

    # Prepend 0 for baseline
    s = pd.Series([0] + list(series))
    cummax = s.cummax()
    dd = s - cummax
    mdd = dd.min()
    mdd_idx = dd.idxmin()

    if mdd_idx > 0 and mdd_idx < len(s):
        peak_idx = cummax[:mdd_idx].idxmax()
        return abs(mdd), peak_idx, mdd_idx

    return 0, None, None


def calc_streaks(results):
    """Calculate win/loss streaks from result list"""
    if not results:
        return 0, 0, 0, 0

    streaks = []
    current_streak = 1
    current_type = results[0] if len(results) > 0 else 'LOSS'

    for i in range(1, len(results)):
        if results[i] == current_type:
            current_streak += 1
        else:
            streaks.append((current_type, current_streak))
            current_type = results[i]
            current_streak = 1
    streaks.append((current_type, current_streak))

    win_streaks = [s[1] for s in streaks if s[0] == 'WIN']
    loss_streaks = [s[1] for s in streaks if s[0] == 'LOSS']

    max_win = max(win_streaks) if win_streaks else 0
    max_loss = max(loss_streaks) if loss_streaks else 0
    avg_win = sum(win_streaks) / len(win_streaks) if win_streaks else 0
    avg_loss = sum(loss_streaks) / len(loss_streaks) if loss_streaks else 0

    return max_win, max_loss, avg_win, avg_loss


# ============================================================================
# DATA LOADING (with caching)
# ============================================================================

@st.cache_data(ttl=300)
def load_latest_scan():
    """Load latest scan metadata"""
    try:
        scan_file = DATA_DIR / 'latest_scan.json'
        if scan_file.exists():
            with open(scan_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        st.error(f"Error loading latest_scan.json: {e}")

    return {
        'scan_date': '',
        'scan_time': '',
        'strategy_a_count': 0,
        'strategy_b_count': 0,
        'strategy_c_count': 0,
        'strategy_d_count': 0,
        'strategy_e_count': 0,
        'total_count': 0
    }


@st.cache_data(ttl=300)
def load_tracker_summary():
    """Load tracker summary stats"""
    try:
        summary_file = DATA_DIR / 'tracker_summary.json'
        if summary_file.exists():
            with open(summary_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        st.error(f"Error loading tracker_summary.json: {e}")

    return {
        'last_tracked': '',
        'open_count': 0,
        'pending_count': 0,
        'closed_count': 0,
        'win_count': 0,
        'loss_count': 0,
        'expired_count': 0,
        'trailing_count': 0
    }


@st.cache_data(ttl=300)
def load_todays_signals():
    """Load today's signals"""
    try:
        today = datetime.now(tz=KST).strftime('%Y-%m-%d')
        signal_file = DATA_DIR / f'signal_{today}.csv'

        if signal_file.exists():
            df = pd.read_csv(signal_file, dtype=str)
            if len(df) > 0:
                return df
    except Exception as e:
        st.error(f"Error loading today's signals: {e}")

    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_open_positions():
    """Load all open positions"""
    try:
        pos_file = DATA_DIR / 'open_positions.csv'
        if pos_file.exists():
            df = pd.read_csv(pos_file, dtype=str)
            if len(df) > 0:
                return df
    except Exception as e:
        st.error(f"Error loading open_positions.csv: {e}")

    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_closed_positions():
    """Load ALL closed position history (no .head(20) limit)"""
    try:
        closed_file = DATA_DIR / 'closed_positions.csv'
        if closed_file.exists():
            df = pd.read_csv(closed_file, dtype=str)
            if len(df) > 0:
                # Sort by close_date descending (newest first)
                try:
                    df['close_date'] = pd.to_datetime(df['close_date'], errors='coerce')
                    df = df.sort_values('close_date', ascending=False)
                except Exception:
                    pass
                return df  # Return ALL, not just head(20)
    except Exception as e:
        st.error(f"Error loading closed_positions.csv: {e}")

    return pd.DataFrame()


# ============================================================================
# CSS & STYLING
# ============================================================================

def get_custom_css():
    """Generate custom CSS for luxury dashboard"""
    css = f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            background-color: {COLOR_PALETTE['bg_deep']};
            color: {COLOR_PALETTE['text_primary']};
            font-family: 'Plus Jakarta Sans', sans-serif;
        }}

        .stApp {{
            background-color: {COLOR_PALETTE['bg_deep']};
        }}

        .stTabs [data-baseweb="tab-list"] {{
            display: none;
        }}

        /* HEADER SECTION */
        .header-main {{
            padding: 2rem 1rem;
            border-bottom: 1px solid {COLOR_PALETTE['border_subtle']};
            margin-bottom: 2rem;
        }}

        .header-title {{
            font-family: 'Cormorant Garamond', serif;
            font-size: 3.5rem;
            font-weight: 700;
            letter-spacing: 0.15em;
            color: {COLOR_PALETTE['text_primary']};
            margin-bottom: 0.5rem;
        }}

        .header-meta {{
            font-family: 'Space Mono', monospace;
            font-size: 0.85rem;
            color: {COLOR_PALETTE['text_secondary']};
            letter-spacing: 0.05em;
        }}

        .meta-item {{
            display: inline-block;
            margin-right: 1.5rem;
        }}

        .meta-value {{
            color: {COLOR_PALETTE['gold']};
            font-weight: 600;
        }}

        /* SECTION HEADERS */
        .section-header {{
            display: flex;
            align-items: center;
            margin: 2.5rem 0 1.5rem 0;
            padding: 0 1rem;
            gap: 1rem;
        }}

        .section-label {{
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            color: {COLOR_PALETTE['text_muted']};
            text-transform: uppercase;
            white-space: nowrap;
        }}

        .section-line {{
            flex: 1;
            height: 1px;
            background-color: {COLOR_PALETTE['border_subtle']};
        }}

        /* NO SIGNALS STATE */
        .no-signals {{
            padding: 1.5rem 1rem;
            color: {COLOR_PALETTE['text_muted']};
            font-style: italic;
            text-align: center;
        }}

        /* SIGNAL CARDS */
        .signal-card {{
            background-color: {COLOR_PALETTE['bg_card']};
            border: 1px solid {COLOR_PALETTE['border_subtle']};
            border-radius: 0.5rem;
            padding: 1.5rem;
            margin-bottom: 1rem;
            transition: all 0.3s ease;
        }}

        .signal-card:hover {{
            background-color: {COLOR_PALETTE['bg_elevated']};
            border-color: {COLOR_PALETTE['gold']};
        }}

        .signal-header {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 1rem;
        }}

        .signal-badge {{
            display: inline-block;
            padding: 0.35rem 0.6rem;
            border-radius: 0.3rem;
            font-family: 'Space Mono', monospace;
            font-size: 0.75rem;
            font-weight: 700;
            color: white;
        }}

        .signal-ticker {{
            font-family: 'Space Mono', monospace;
            font-size: 1.5rem;
            font-weight: 700;
            color: {COLOR_PALETTE['text_primary']};
        }}

        .signal-price {{
            font-family: 'Space Mono', monospace;
            font-size: 1.25rem;
            color: {COLOR_PALETTE['gold']};
            font-weight: 600;
        }}

        .signal-target {{
            font-size: 0.95rem;
            color: {COLOR_PALETTE['text_secondary']};
            margin-top: 0.5rem;
        }}

        .signal-meta {{
            font-size: 0.85rem;
            color: {COLOR_PALETTE['text_muted']};
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid {COLOR_PALETTE['border_subtle']};
        }}

        /* POSITION CARDS */
        .pos-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1rem;
            padding: 0 1rem;
            margin-bottom: 1rem;
        }}

        .pos-card {{
            background-color: {COLOR_PALETTE['bg_card']};
            border: 1px solid {COLOR_PALETTE['border_subtle']};
            border-radius: 0.5rem;
            padding: 1.25rem;
            transition: all 0.3s ease;
        }}

        .pos-card:hover {{
            background-color: {COLOR_PALETTE['bg_elevated']};
            border-color: {COLOR_PALETTE['text_secondary']};
        }}

        .pos-card.pos-warn {{
            border-left: 3px solid {COLOR_PALETTE['red']};
        }}

        .pos-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1rem;
            padding-bottom: 0.75rem;
            border-bottom: 1px solid {COLOR_PALETTE['border_subtle']};
        }}

        .pos-header-left {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .pos-ticker {{
            font-family: 'Space Mono', monospace;
            font-size: 1.1rem;
            font-weight: 700;
            color: {COLOR_PALETTE['text_primary']};
        }}

        .pos-days {{
            font-family: 'Space Mono', monospace;
            font-size: 0.8rem;
            color: {COLOR_PALETTE['text_muted']};
        }}

        .pos-prices {{
            font-family: 'Space Mono', monospace;
            font-size: 0.95rem;
            color: {COLOR_PALETTE['text_secondary']};
            margin-bottom: 0.75rem;
        }}

        .pos-change {{
            font-weight: 600;
            margin-left: 0.5rem;
        }}

        .pos-change.c-win {{
            color: {COLOR_PALETTE['green']};
        }}

        .pos-change.c-loss {{
            color: {COLOR_PALETTE['red']};
        }}

        .pos-progress {{
            height: 6px;
            background-color: {COLOR_PALETTE['bg_elevated']};
            border-radius: 3px;
            overflow: hidden;
            margin-bottom: 0.75rem;
        }}

        .pos-progress-bar {{
            height: 100%;
            background-color: {COLOR_PALETTE['gold']};
            transition: width 0.3s ease;
            border-radius: 3px;
        }}

        .pos-meta {{
            display: flex;
            justify-content: space-between;
            font-size: 0.8rem;
            color: {COLOR_PALETTE['text_muted']};
        }}

        /* RESULT ROWS */
        .result-row {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid {COLOR_PALETTE['border_subtle']};
            font-family: 'Space Mono', monospace;
            font-size: 0.9rem;
        }}

        .result-row:last-child {{
            border-bottom: none;
        }}

        .result-left {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex: 1;
        }}

        .result-ticker {{
            font-weight: 700;
            color: {COLOR_PALETTE['text_primary']};
            min-width: 60px;
        }}

        .result-status {{
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}

        .result-status.win {{
            background-color: {COLOR_PALETTE['green']}22;
            color: {COLOR_PALETTE['green']};
        }}

        .result-status.loss {{
            background-color: {COLOR_PALETTE['red']}22;
            color: {COLOR_PALETTE['red']};
        }}

        .result-status.expired {{
            background-color: {COLOR_PALETTE['text_muted']}22;
            color: {COLOR_PALETTE['text_secondary']};
        }}

        .result-right {{
            text-align: right;
            color: {COLOR_PALETTE['text_secondary']};
        }}

        /* TREND DOTS */
        .trend-header {{
            padding: 1rem;
            background-color: {COLOR_PALETTE['bg_elevated']};
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }}

        .trend-row {{
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 0.75rem;
            font-size: 0.85rem;
        }}

        .trend-row:last-child {{
            margin-bottom: 0;
        }}

        .trend-dots {{
            display: flex;
            gap: 0.3rem;
            flex: 1;
        }}

        .dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
        }}

        .dot-win {{
            background-color: {COLOR_PALETTE['green']};
        }}

        .dot-loss {{
            background-color: {COLOR_PALETTE['red']};
        }}

        .dot-neutral {{
            background-color: {COLOR_PALETTE['text_muted']};
        }}

        .trend-rate {{
            font-family: 'Space Mono', monospace;
            font-weight: 600;
            color: {COLOR_PALETTE['gold']};
            min-width: 45px;
            text-align: right;
        }}

        /* STATS GRID */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            padding: 1rem;
            margin-bottom: 1rem;
        }}

        .stat-box {{
            background-color: {COLOR_PALETTE['bg_card']};
            border: 1px solid {COLOR_PALETTE['border_subtle']};
            border-radius: 0.5rem;
            padding: 1rem;
            text-align: center;
        }}

        .stat-label {{
            font-size: 0.75rem;
            color: {COLOR_PALETTE['text_muted']};
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }}

        .stat-value {{
            font-family: 'Space Mono', monospace;
            font-size: 1.5rem;
            font-weight: 700;
            color: {COLOR_PALETTE['gold']};
        }}

        /* STRATEGY BREAKDOWN */
        .strategy-breakdown {{
            background-color: {COLOR_PALETTE['bg_elevated']};
            border-radius: 0.5rem;
            padding: 1rem;
            margin-bottom: 1rem;
        }}

        .strat-row {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0.75rem 0;
            border-bottom: 1px solid {COLOR_PALETTE['border_subtle']};
            font-size: 0.9rem;
        }}

        .strat-row:last-child {{
            border-bottom: none;
        }}

        .strat-name {{
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-weight: 600;
            color: {COLOR_PALETTE['text_primary']};
        }}

        .strat-stat {{
            color: {COLOR_PALETTE['text_secondary']};
            font-family: 'Space Mono', monospace;
            font-size: 0.85rem;
        }}

        /* Strategy Tags */
        .stag {{ display:inline-block; padding:3px 8px; border-radius:4px; font-family:'Space Mono',monospace; font-weight:700; font-size:0.74em; letter-spacing:0.02em; border:1px solid; white-space:nowrap; }}
        .stag-A {{ color:#5cc49a; border-color:#4a9e7d; background:#4a9e7d12; }}
        .stag-B {{ color:#4a7a9e; border-color:#4a7a9e66; background:#4a7a9e0a; }}
        .stag-C {{ color:#b8954a; border-color:#b8954a66; background:#b8954a0a; }}
        .stag-D {{ color:#c46b7c; border-color:#9e4a5a; background:#9e4a5a0a; }}
        .stag-E {{ color:#9a7abf; border-color:#7a5aaf66; background:#7a5aaf0a; }}

        /* Cell States */
        .c-win {{ color:#5cc49a; font-weight:600; }}
        .c-loss {{ color:#c46b7c; font-weight:600; }}
        .c-partial {{ color:#b8954a; font-weight:500; }}
        .c-pending {{ color:#4d5b72; font-style:italic; }}
        .c-none {{ color:#2a3444; }}
        .c-muted {{ color:#4d5b72; }}

        /* Status badges */
        .st-win {{ color:#5cc49a; font-weight:600; }}
        .st-loss {{ color:#c46b7c; font-weight:600; }}
        .st-expired {{ color:#b8954a; font-weight:600; }}
        .st-open {{ color:#4a7a9e; font-weight:600; }}
        .st-pending {{ color:#4d5b72; font-weight:500; }}

        /* Progress bar */
        .rr-prog {{ display:flex; align-items:center; gap:6px; }}
        .rr-prog-track {{ flex:1; background:#1a2230; border-radius:3px; height:6px; min-width:40px; overflow:hidden; }}
        .rr-prog-fill {{ height:100%; border-radius:3px; }}
        .rr-prog-text {{ font-size:0.78em; font-weight:600; min-width:32px; text-align:right; }}

        /* RR-styled tables, cards, stats for Performance section */
        .rr-table {{ width:100%; border-collapse:separate; border-spacing:0; font-family:'Plus Jakarta Sans',sans-serif; font-size:0.84em; background:#131920; border-radius:10px; overflow:hidden; border:1px solid #1c2636; }}
        .rr-table th {{ background:#1a2230; color:#4d5b72; padding:12px 14px; text-align:center; font-weight:500; font-size:0.82em; letter-spacing:0.04em; text-transform:uppercase; border-bottom:1px solid #1c2636; }}
        .rr-table th:first-child {{ text-align:left; padding-left:20px; }}
        .rr-table td {{ padding:12px 14px; text-align:center; border-bottom:1px solid #0d131a; color:#e8e4de; font-variant-numeric:tabular-nums; font-family:'Space Mono',monospace; font-size:0.92em; }}
        .rr-table td:first-child {{ text-align:left; padding-left:20px; font-family:'Plus Jakarta Sans',sans-serif; }}
        .rr-table tr:last-child td {{ border-bottom:none; }}
        .rr-table tr:hover td {{ background:#1e2a3a; }}
        .rr-table-wrap {{ overflow-x:auto; -webkit-overflow-scrolling:touch; }}

        .rr-cards {{ display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin-bottom:24px; }}
        .rr-card {{ background:#131920; border:1px solid #1c2636; border-radius:10px; padding:20px 16px; text-align:center; position:relative; overflow:hidden; }}
        .rr-card::before {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; background:#1c2636; }}
        .rr-card.accent::before {{ background:linear-gradient(90deg,#9a7d4e,#c9a96e,#9a7d4e); }}
        .rr-card .val {{ font-family:'Space Mono',monospace; font-size:1.8em; font-weight:700; line-height:1.2; margin-bottom:4px; }}
        .rr-card .lbl {{ font-family:'Plus Jakarta Sans',sans-serif; font-size:0.72em; color:#4d5b72; letter-spacing:0.08em; text-transform:uppercase; }}

        .rr-stats {{ display:flex; gap:12px; flex-wrap:wrap; margin-bottom:20px; }}
        .rr-stat {{ background:#131920; border:1px solid #1c2636; border-radius:8px; padding:16px 20px; flex:1; min-width:120px; text-align:center; }}
        .rr-stat .s-label {{ font-family:'Plus Jakarta Sans',sans-serif; font-size:0.72em; color:#4d5b72; letter-spacing:0.06em; text-transform:uppercase; margin-bottom:4px; }}
        .rr-stat .s-value {{ font-family:'Space Mono',monospace; font-size:1.5em; font-weight:700; }}

        .rr-empty {{ text-align:center; padding:48px 20px; color:#4d5b72; font-size:0.9em; background:#131920; border-radius:10px; border:1px solid #1c2636; }}
        .rr-legend {{ font-family:'Plus Jakarta Sans',sans-serif; font-size:0.76em; color:#4d5b72; margin:8px 0 12px 0; letter-spacing:0.02em; }}
        .rr-divider {{ width:60px; height:1px; background:linear-gradient(90deg,transparent,#c9a96e,transparent); margin:16px auto; }}

        /* Tab overrides for Performance sub-tabs */
        .perf-tabs .stTabs [data-baseweb="tab-list"] {{ display:flex !important; gap:0; background:#0d1117; border-radius:8px; padding:4px; border:1px solid #1c2636; }}
        .perf-tabs .stTabs [data-baseweb="tab"] {{ font-family:'Plus Jakarta Sans',sans-serif !important; font-weight:500 !important; font-size:0.82em !important; color:#4d5b72 !important; padding:8px 16px !important; border-radius:6px !important; border:none !important; background:transparent !important; white-space:nowrap !important; }}
        .perf-tabs .stTabs [data-baseweb="tab"][aria-selected="true"] {{ color:#c9a96e !important; background:#1a2230 !important; }}
        .perf-tabs .stTabs [data-baseweb="tab-highlight"] {{ display:none !important; }}
        .perf-tabs .stTabs [data-baseweb="tab-border"] {{ display:none !important; }}

        /* RESPONSIVE */
        @media (max-width: 768px) {{
            .header-title {{
                font-size: 2.5rem;
            }}

            .header-meta {{
                font-size: 0.75rem;
            }}

            .meta-item {{
                margin-right: 1rem;
                display: block;
                margin-bottom: 0.5rem;
            }}

            .pos-grid {{
                grid-template-columns: 1fr;
            }}

            .stats-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}

            .rr-cards {{ grid-template-columns:repeat(2,1fr); gap:8px; }}
            .rr-card {{ padding:14px 10px; }}
            .rr-card .val {{ font-size:1.4em; }}
            .rr-table {{ font-size:0.76em; min-width:600px; }}
            .rr-table th,.rr-table td {{ padding:10px; white-space:nowrap; }}
            .rr-stats {{ flex-wrap:wrap; }}
            .rr-stat {{ min-width:calc(50% - 8px); flex:unset; padding:12px 14px; }}
            .rr-stat .s-value {{ font-size:1.2em; }}
        }}

        @media (max-width: 480px) {{
            .header-title {{
                font-size: 1.75rem;
                letter-spacing: 0.1em;
            }}

            .signal-card {{
                padding: 1rem;
            }}

            .signal-ticker {{
                font-size: 1.25rem;
            }}

            .stats-grid {{
                grid-template-columns: 1fr;
            }}

            .section-header {{
                padding: 0;
            }}
        }}
    </style>
    """
    return css


# ============================================================================
# PAGE SECTIONS
# ============================================================================

def render_header():
    """Render page header with metadata"""
    latest_scan = load_latest_scan()
    tracker = load_tracker_summary()

    scan_date = safe_str(latest_scan.get('scan_date', ''))
    scan_time = safe_str(latest_scan.get('scan_time', ''))
    win_count = safe_float(tracker.get('win_count', 0))
    open_count = safe_float(tracker.get('open_count', 0))

    html = f"""
    <div class="header-main">
        <div class="header-title">SURGE SCANNER</div>
        <div class="header-meta">
            <span class="meta-item">Last Scan: <span class="meta-value">{scan_date} {scan_time}</span></span>
            <span class="meta-item">Active: <span class="meta-value">{int(open_count)}</span></span>
            <span class="meta-item">Wins: <span class="meta-value">{int(win_count)}</span></span>
        </div>
    </div>
    """

    st.html(html)


def render_section_header(label):
    """Render section header with line"""
    html = f"""
    <div class="section-header">
        <span class="section-label">{label}</span>
        <div class="section-line"></div>
    </div>
    """
    st.html(html)


def render_todays_signals():
    """Render today's signals section"""
    render_section_header("TODAY'S SIGNALS")

    signals_df = load_todays_signals()

    if len(signals_df) == 0:
        st.html('<div class="no-signals">No signals today</div>')
        return

    # Display signals as cards
    cols = st.columns([1, 1] if len(signals_df) > 1 else [1])

    for idx, (_, row) in enumerate(signals_df.iterrows()):
        try:
            strategy = safe_str(row.get('strategy', ''))
            ticker = safe_str(row.get('ticker', ''))
            price = safe_float(row.get('price', 0))
            tp_price = safe_float(row.get('tp_price', 0))

            # Strategy-specific metadata
            rsi = safe_float(row.get('rsi', 0)) if 'rsi' in row else None
            ret_5d = safe_float(row.get('ret_5d', 0)) if 'ret_5d' in row else None
            intra_high = safe_float(row.get('intra_high_pct', 0)) if 'intra_high_pct' in row else None

            # Determine metric to show
            metric_text = ''
            if strategy == 'A' and rsi is not None:
                metric_text = f"RSI {rsi:.1f}"
            elif strategy in ['B', 'C', 'D'] and ret_5d is not None:
                metric_text = f"5D Return {format_percent(ret_5d)}"
            elif strategy == 'E' and intra_high is not None:
                metric_text = f"Intra {format_percent(intra_high)}"

            if intra_high is not None and metric_text:
                metric_text += f" · Intra {format_percent(intra_high)}"

            color = get_strategy_color(strategy)
            strat_name = get_strategy_name(strategy)

            card_html = f"""
            <div class="signal-card">
                <div class="signal-header">
                    <span class="signal-badge" style="background-color: {color}">{strategy}</span>
                    <span class="signal-ticker">{ticker}</span>
                </div>
                <div class="signal-price">{format_currency(price)}</div>
                <div class="signal-target">Target {format_currency(tp_price)}</div>
                <div class="signal-meta">{strat_name} · {metric_text}</div>
            </div>
            """

            col_idx = idx % 2
            with cols[col_idx]:
                st.html(card_html)

        except Exception as e:
            st.error(f"Error rendering signal: {e}")


def render_active_positions():
    """Render active positions section sorted by risk"""
    render_section_header("ACTIVE POSITIONS")

    open_pos_df = load_open_positions()

    if len(open_pos_df) == 0:
        st.html('<div class="no-signals">No active positions</div>')
        return

    # Split into OPEN and PENDING
    open_positions = open_pos_df[
        (open_pos_df.get('status') == 'OPEN') |
        (open_pos_df.get('status').isna())
    ].copy()

    pending_positions = open_pos_df[
        open_pos_df.get('status') == 'PENDING'
    ].copy()

    # Calculate risk scores and sort
    try:
        open_positions['risk_score'] = open_positions.apply(calc_risk_score, axis=1)
        open_positions = open_positions.sort_values('risk_score', ascending=False)
    except Exception:
        pass

    # Render OPEN positions
    if len(open_positions) > 0:
        st.html('<div style="font-size: 0.85rem; color: #8a9ab5; margin-left: 1rem; margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">Open Positions</div>')

        cols = st.columns([1, 1] if len(open_positions) > 1 else [1])

        for idx, (_, row) in enumerate(open_positions.iterrows()):
            try:
                strategy = safe_str(row.get('strategy', ''))
                ticker = safe_str(row.get('ticker', ''))
                entry_price = safe_float(row.get('entry_price', 0))
                current_price = safe_float(row.get('current_price', 0))
                tp_price = safe_float(row.get('tp_price', 0))
                sl_price = safe_float(row.get('sl_price', 0))
                days_held = safe_float(row.get('days_held', 0))
                max_hold = safe_float(row.get('max_hold', 0))
                achievement_pct = safe_float(row.get('achievement_pct', 0))
                change_pct = safe_float(row.get('change_pct', 0))
                risk_score = safe_float(row.get('risk_score', 0))

                # Check if position is at risk
                is_at_risk = risk_score > 25

                # Calculate progress toward TP
                if tp_price > entry_price:
                    progress_pct = min(100, max(0, ((current_price - entry_price) / (tp_price - entry_price)) * 100))
                else:
                    progress_pct = 0

                color = get_strategy_color(strategy)
                change_class = 'c-win' if change_pct >= 0 else 'c-loss'
                card_warn_class = 'pos-warn' if is_at_risk else ''

                card_html = f"""
                <div class="pos-card {card_warn_class}">
                    <div class="pos-header">
                        <div class="pos-header-left">
                            <span class="signal-badge" style="background-color: {color}">{strategy}</span>
                            <span class="pos-ticker">{ticker}</span>
                        </div>
                        <span class="pos-days">D{int(days_held)}/{int(max_hold)}</span>
                    </div>
                    <div class="pos-body">
                        <div class="pos-prices">
                            {format_currency(entry_price)} → {format_currency(current_price)}
                            <span class="pos-change {change_class}">{format_percent(change_pct)}</span>
                        </div>
                        <div class="pos-progress">
                            <div class="pos-progress-bar" style="width: {progress_pct}%"></div>
                        </div>
                        <div class="pos-meta">
                            <span>SL {format_currency(sl_price)}</span>
                            <span>TP {format_currency(tp_price)}</span>
                        </div>
                    </div>
                </div>
                """

                col_idx = idx % 2
                with cols[col_idx]:
                    st.html(card_html)

            except Exception as e:
                st.error(f"Error rendering position: {e}")

    # Render PENDING positions
    if len(pending_positions) > 0:
        st.html('<div style="font-size: 0.85rem; color: #8a9ab5; margin-left: 1rem; margin-top: 1.5rem; margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">Pending Positions</div>')

        for _, row in pending_positions.iterrows():
            try:
                strategy = safe_str(row.get('strategy', ''))
                ticker = safe_str(row.get('ticker', ''))
                signal_price = safe_float(row.get('signal_price', 0))
                signal_date = safe_str(row.get('signal_date', ''))

                color = get_strategy_color(strategy)

                pending_html = f"""
                <div class="pos-card" style="opacity: 0.7;">
                    <div class="pos-header">
                        <div class="pos-header-left">
                            <span class="signal-badge" style="background-color: {color}">{strategy}</span>
                            <span class="pos-ticker">{ticker}</span>
                        </div>
                    </div>
                    <div style="font-size: 0.85rem; color: {COLOR_PALETTE['text_muted']};">
                        Signal {signal_price} on {signal_date} · Awaiting entry
                    </div>
                </div>
                """

                st.html(pending_html)

            except Exception:
                pass


def render_recent_results():
    """Render recent results section with win-rate trends"""
    render_section_header("RECENT RESULTS")

    closed_pos_df = load_closed_positions()

    if len(closed_pos_df) == 0:
        st.html('<div class="no-signals">No closed positions yet</div>')
        return

    # Build trend data per strategy (last 10 results)
    strategy_trends = {}

    for strategy in ['A', 'B', 'C', 'D', 'E']:
        strat_results = closed_pos_df[closed_pos_df['strategy'] == strategy].head(10)
        if len(strat_results) > 0:
            wins = len(strat_results[strat_results['result_status'] == 'WIN'])
            win_rate = (wins / len(strat_results)) * 100 if len(strat_results) > 0 else 0

            # Build dot sequence (oldest to newest)
            dots_html = ''
            for _, res_row in strat_results.iloc[::-1].iterrows():
                result_status = safe_str(res_row.get('result_status', ''))
                if result_status == 'WIN':
                    dots_html += '<span class="dot dot-win"></span>'
                elif result_status == 'LOSS':
                    dots_html += '<span class="dot dot-loss"></span>'
                else:
                    dots_html += '<span class="dot dot-neutral"></span>'

            strategy_trends[strategy] = {
                'win_rate': win_rate,
                'dots': dots_html,
                'count': len(strat_results)
            }

    # Render trend header
    if strategy_trends:
        trend_html = '<div class="trend-header">'
        for strategy in ['A', 'B', 'C', 'D', 'E']:
            if strategy in strategy_trends:
                data = strategy_trends[strategy]
                color = get_strategy_color(strategy)
                trend_html += f"""
                <div class="trend-row">
                    <span class="signal-badge" style="background-color: {color}">{strategy}</span>
                    <div class="trend-dots">{data['dots']}</div>
                    <span class="trend-rate">{data['win_rate']:.0f}%</span>
                </div>
                """
        trend_html += '</div>'
        st.html(trend_html)

    # Render result rows
    st.html('<div style="margin-top: 1rem;"></div>')

    for _, row in closed_pos_df.head(20).iterrows():
        try:
            strategy = safe_str(row.get('strategy', ''))
            ticker = safe_str(row.get('ticker', ''))
            result_status = safe_str(row.get('result_status', ''))
            result_pct = safe_float(row.get('result_pct', 0))
            days_held = safe_float(row.get('days_held', 0))

            color = get_strategy_color(strategy)
            status_class = result_status.lower()

            result_html = f"""
            <div class="result-row">
                <div class="result-left">
                    <span class="signal-badge" style="background-color: {color}">{strategy}</span>
                    <span class="result-ticker">{ticker}</span>
                    <span class="result-status {status_class}">{result_status}</span>
                </div>
                <div class="result-right">
                    {format_percent(result_pct)} · {int(days_held)}d
                </div>
            </div>
            """

            st.html(result_html)

        except Exception:
            pass


def render_performance():
    """Render comprehensive performance analytics with 8 sub-tabs"""
    render_section_header("PERFORMANCE")

    open_pos = load_open_positions()
    closed_pos = load_closed_positions()
    tracker = load_tracker_summary()

    # Build all_records from both open and closed positions
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

    # Create tabs for performance section
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "Daily Matrix", "Monthly Matrix", "Win Rates", "Active Pos",
        "Closed Pos", "By Ticker", "P&L & MDD", "Analytics"
    ])

    with tab1:
        render_daily_matrix(all_records)

    with tab2:
        render_monthly_matrix(all_records)

    with tab3:
        render_strategy_win_rates(closed_pos)

    with tab4:
        render_active_detailed(open_pos)

    with tab5:
        render_closed_detailed(closed_pos)

    with tab6:
        render_by_ticker(closed_pos)

    with tab7:
        render_pnl_mdd(closed_pos)

    with tab8:
        render_analytics_suite(closed_pos, open_pos)


def render_daily_matrix(all_records):
    """30-day daily matrix view"""
    if all_records.empty:
        st.markdown('<div class="rr-empty">No records for matrix</div>', unsafe_allow_html=True)
        return

    # Get last 30 days
    end_date = all_records['signal_date'].max()
    start_date = end_date - pd.Timedelta(days=30)
    records_30 = all_records[(all_records['signal_date'] >= start_date) & (all_records['signal_date'] <= end_date)].copy()

    if records_30.empty:
        st.markdown('<div class="rr-empty">No data in last 30 days</div>', unsafe_allow_html=True)
        return

    # Build matrix
    records_30['date_only'] = records_30['signal_date'].dt.date
    matrix_data = []

    for strategy in ['A', 'B', 'C', 'D', 'E']:
        strat_records = records_30[records_30['strategy'] == strategy]
        if not strat_records.empty:
            row_data = {'Strategy': stag(strategy)}

            for date in pd.date_range(start_date, end_date):
                date_records = strat_records[strat_records['date_only'] == date.date()]

                if len(date_records) > 0:
                    wins = len(date_records[date_records['result'] == 'WIN'])
                    losses = len(date_records[date_records['result'] == 'LOSS'])
                    total = len(date_records)

                    if losses > 0:
                        cell = f"L {losses}"
                        cls = "c-loss"
                    elif wins == total:
                        cell = f"W {wins}"
                        cls = "c-win"
                    elif wins > 0:
                        cell = f"P {wins}"
                        cls = "c-partial"
                    else:
                        cell = f"Act"
                        cls = "c-pending"

                    row_data[date.strftime('%m/%d')] = f'<span class="{cls}">{cell}</span>'
                else:
                    row_data[date.strftime('%m/%d')] = '<span class="c-none">—</span>'

            matrix_data.append(row_data)

    if matrix_data:
        matrix_df = pd.DataFrame(matrix_data)
        st.markdown(f'<div class="rr-table-wrap"><table class="rr-table">', unsafe_allow_html=True)

        # Header
        cols_html = '<tr>' + ''.join([f'<th>{col}</th>' for col in matrix_df.columns]) + '</tr>'
        st.markdown(cols_html, unsafe_allow_html=True)

        # Rows
        for _, row in matrix_df.iterrows():
            row_html = '<tr>'
            for col in matrix_df.columns:
                row_html += f'<td>{row[col]}</td>'
            row_html += '</tr>'
            st.markdown(row_html, unsafe_allow_html=True)

        st.markdown('</table></div>', unsafe_allow_html=True)


def render_monthly_matrix(all_records):
    """Monthly matrix grouped by YYYY-MM"""
    if all_records.empty:
        st.markdown('<div class="rr-empty">No records for matrix</div>', unsafe_allow_html=True)
        return

    # Build matrix by month
    matrix_data = []

    for strategy in ['A', 'B', 'C', 'D', 'E']:
        strat_records = all_records[all_records['strategy'] == strategy]
        if not strat_records.empty:
            row_data = {'Strategy': stag(strategy)}

            for month in sorted(strat_records['month_str'].unique()):
                month_records = strat_records[strat_records['month_str'] == month]
                wins = len(month_records[month_records['result'] == 'WIN'])
                losses = len(month_records[month_records['result'] == 'LOSS'])
                total = len(month_records)

                if losses > 0:
                    cell = f"L {losses}"
                    cls = "c-loss"
                elif wins == total:
                    cell = f"W {wins}"
                    cls = "c-win"
                elif wins > 0:
                    cell = f"P {wins}"
                    cls = "c-partial"
                else:
                    cell = f"Act"
                    cls = "c-pending"

                row_data[month] = f'<span class="{cls}">{cell}</span>'

            matrix_data.append(row_data)

    if matrix_data:
        matrix_df = pd.DataFrame(matrix_data)
        st.markdown(f'<div class="rr-table-wrap"><table class="rr-table">', unsafe_allow_html=True)

        cols_html = '<tr>' + ''.join([f'<th>{col}</th>' for col in matrix_df.columns]) + '</tr>'
        st.markdown(cols_html, unsafe_allow_html=True)

        for _, row in matrix_df.iterrows():
            row_html = '<tr>'
            for col in matrix_df.columns:
                row_html += f'<td>{row[col]}</td>'
            row_html += '</tr>'
            st.markdown(row_html, unsafe_allow_html=True)

        st.markdown('</table></div>', unsafe_allow_html=True)


def render_strategy_win_rates(closed_pos):
    """Strategy win rates table with backtest WR"""
    if closed_pos.empty:
        st.markdown('<div class="rr-empty">No closed positions</div>', unsafe_allow_html=True)
        return

    table_rows = []

    for strategy in ['A', 'B', 'C', 'D', 'E']:
        strat_closed = closed_pos[closed_pos['strategy'] == strategy]
        if not strat_closed.empty:
            total = len(strat_closed)
            wins = len(strat_closed[strat_closed['result_status'] == 'WIN'])
            losses = len(strat_closed[strat_closed['result_status'] == 'LOSS'])
            expired = len(strat_closed[strat_closed['result_status'] == 'EXPIRED'])
            active = 0  # Placeholder
            win_rate = (wins / total * 100) if total > 0 else 0

            pcts = [safe_float(v) for v in strat_closed.get('result_pct', []) if safe_float(v) != 0]
            avg_pnl = sum(pcts) / len(pcts) if pcts else 0

            table_rows.append({
                'Strategy': stag(strategy),
                'Target': STRAT_TP.get(strategy, '—'),
                'Total': total,
                'Closed': total,
                'Win': wins,
                'Loss': losses,
                'Expired': expired,
                'Active': active,
                'Win Rate': f'{win_rate:.1f}%',
                'Backtest': STRAT_BT_WR.get(strategy, '—'),
                'Avg P&L': f'{avg_pnl:+.2f}%'
            })

    if table_rows:
        df = pd.DataFrame(table_rows)
        st.markdown(f'<div class="rr-table-wrap"><table class="rr-table">', unsafe_allow_html=True)

        cols_html = '<tr>' + ''.join([f'<th>{col}</th>' for col in df.columns]) + '</tr>'
        st.markdown(cols_html, unsafe_allow_html=True)

        for _, row in df.iterrows():
            row_html = '<tr>'
            for col in df.columns:
                row_html += f'<td>{row[col]}</td>'
            row_html += '</tr>'
            st.markdown(row_html, unsafe_allow_html=True)

        st.markdown('</table></div>', unsafe_allow_html=True)


def render_active_detailed(open_pos):
    """Detailed active positions table with filters"""
    if open_pos.empty:
        st.markdown('<div class="rr-empty">No active positions</div>', unsafe_allow_html=True)
        return

    col1, col2 = st.columns(2)
    with col1:
        strat_filter = st.selectbox("Filter by Strategy", ['All', 'A', 'B', 'C', 'D', 'E'], key='act_strat')
    with col2:
        sort_by = st.selectbox("Sort by", ['Date', 'P&L', 'Achievement'], key='act_sort')

    filtered_pos = open_pos.copy()
    if strat_filter != 'All':
        filtered_pos = filtered_pos[filtered_pos['strategy'] == strat_filter]

    if filtered_pos.empty:
        st.markdown('<div class="rr-empty">No positions match filters</div>', unsafe_allow_html=True)
        return

    table_rows = []
    for _, row in filtered_pos.iterrows():
        strat = safe_str(row.get('strategy', ''))
        ticker = safe_str(row.get('ticker', ''))
        signal = safe_str(row.get('signal_date', ''))[:10]
        entry = safe_float(row.get('entry_price', 0))
        current = safe_float(row.get('current_price', 0))
        pnl_pct = safe_float(row.get('change_pct', 0))
        tp = safe_float(row.get('tp_price', 0))
        peak = safe_float(row.get('peak_price', 0))
        peak_date = safe_str(row.get('peak_date', ''))[:10]
        ach = safe_float(row.get('achievement_pct', 0))
        remaining = safe_float(row.get('remaining_pct', 0))
        days = safe_float(row.get('days_held', 0))

        table_rows.append({
            'Strat': stag(strat),
            'Ticker': ticker,
            'Signal': signal,
            'Entry': format_currency(entry),
            'Price': format_currency(current),
            'Current': format_currency(current),
            'P&L': format_percent(pnl_pct),
            'TP': format_currency(tp),
            'Peak': format_currency(peak),
            'Peak Date': peak_date,
            'Achievement': f'{ach:.0f}%',
            'Remaining': f'{remaining:.0f}%',
            'Days': f'D{int(days)}'
        })

    if table_rows:
        df = pd.DataFrame(table_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_closed_detailed(closed_pos):
    """Detailed closed positions table with filters"""
    if closed_pos.empty:
        st.markdown('<div class="rr-empty">No closed positions</div>', unsafe_allow_html=True)
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        strat_filter = st.selectbox("Filter by Strategy", ['All', 'A', 'B', 'C', 'D', 'E'], key='cl_strat')
    with col2:
        result_filter = st.selectbox("Filter by Result", ['All', 'WIN', 'LOSS', 'EXPIRED'], key='cl_result')
    with col3:
        sort_by = st.selectbox("Sort by", ['Recent', 'P&L'], key='cl_sort')

    filtered_pos = closed_pos.copy()
    if strat_filter != 'All':
        filtered_pos = filtered_pos[filtered_pos['strategy'] == strat_filter]
    if result_filter != 'All':
        rc = 'result_status' if 'result_status' in filtered_pos.columns else 'status'
        filtered_pos = filtered_pos[filtered_pos[rc] == result_filter]

    if filtered_pos.empty:
        st.markdown('<div class="rr-empty">No positions match filters</div>', unsafe_allow_html=True)
        return

    table_rows = []
    rc = 'result_status' if 'result_status' in filtered_pos.columns else 'status'

    for _, row in filtered_pos.iterrows():
        strat = safe_str(row.get('strategy', ''))
        ticker = safe_str(row.get('ticker', ''))
        signal = safe_str(row.get('signal_date', ''))[:10]
        entry = safe_float(row.get('entry_price', 0))
        result = safe_str(row.get(rc, ''))
        close = safe_str(row.get('close_date', ''))[:10]
        close_price = safe_float(row.get('close_price', 0))
        pnl = safe_float(row.get('result_pct', 0))
        tp_hit = safe_str(row.get('tp_hit', '')) or '—'
        peak = safe_float(row.get('peak_price', 0))
        peak_date = safe_str(row.get('peak_date', ''))[:10]
        ach = safe_float(row.get('achievement_pct', 0))

        table_rows.append({
            'Strat': stag(strat),
            'Ticker': ticker,
            'Signal': signal,
            'Entry': format_currency(entry),
            'Price': format_currency(entry),
            'Result': result,
            'Close': close,
            'Close$': format_currency(close_price),
            'P&L': format_percent(pnl),
            'TP Hit': tp_hit,
            'Peak': format_currency(peak),
            'Peak Date': peak_date,
            'Ach%': f'{ach:.0f}%'
        })

    if table_rows:
        df = pd.DataFrame(table_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_by_ticker(closed_pos):
    """Group by ticker with win rates"""
    if closed_pos.empty:
        st.markdown('<div class="rr-empty">No closed positions</div>', unsafe_allow_html=True)
        return

    rc = 'result_status' if 'result_status' in closed_pos.columns else 'status'

    ticker_stats = {}
    for _, row in closed_pos.iterrows():
        ticker = safe_str(row.get('ticker', ''))
        result = safe_str(row.get(rc, ''))

        if ticker not in ticker_stats:
            ticker_stats[ticker] = {'total': 0, 'wins': 0}

        ticker_stats[ticker]['total'] += 1
        if result == 'WIN':
            ticker_stats[ticker]['wins'] += 1

    table_rows = []
    for ticker in sorted(ticker_stats.keys()):
        stats = ticker_stats[ticker]
        win_rate = (stats['wins'] / stats['total'] * 100) if stats['total'] > 0 else 0
        color = COLOR_PALETTE['green'] if win_rate >= 50 else COLOR_PALETTE['red']

        table_rows.append({
            'Ticker': ticker,
            'Count': stats['total'],
            'Wins': stats['wins'],
            'Win Rate': f'{win_rate:.1f}%',
            'Color': color
        })

    if table_rows:
        for row in table_rows:
            st.markdown(f"**{row['Ticker']}** | Count: {row['Count']} | Wins: {row['Wins']} | "
                       f"<span style='color:{row['Color']}'>{row['Win Rate']}</span>", unsafe_allow_html=True)


def render_pnl_mdd(closed_pos):
    """P&L and Max Drawdown visualization with per-strategy charts"""
    if closed_pos.empty or 'close_date' not in closed_pos.columns or 'result_pct' not in closed_pos.columns:
        st.markdown('<div class="rr-empty">No closed positions for P&L</div>', unsafe_allow_html=True)
        return

    rc = 'result_status' if 'result_status' in closed_pos.columns else 'status'

    pnl_df = closed_pos[['strategy', 'close_date', 'result_pct']].copy()
    pnl_df['result_pct'] = pd.to_numeric(pnl_df['result_pct'], errors='coerce')
    pnl_df['close_date'] = pd.to_datetime(pnl_df['close_date'], errors='coerce')
    pnl_df = pnl_df.dropna(subset=['result_pct', 'close_date']).sort_values('close_date')

    if pnl_df.empty:
        st.markdown('<div class="rr-empty">No valid P&L data</div>', unsafe_allow_html=True)
        return

    strats_data = sorted(pnl_df['strategy'].unique())
    sc = {'A': '#4a9e7d', 'B': '#4a7a9e', 'C': '#b8954a', 'D': '#9e4a5a', 'E': '#7a5aaf'}

    # Build per-strategy cumulative P&L
    chart_data = pd.DataFrame()
    for s in strats_data:
        sd = pnl_df[pnl_df['strategy'] == s].copy().sort_values('close_date')
        sd[s] = sd['result_pct'].cumsum()
        daily = sd.groupby('close_date')[s].last()
        chart_data = daily.to_frame() if chart_data.empty else chart_data.join(daily, how='outer')

    total = pnl_df.sort_values('close_date').copy()
    total['Total'] = total['result_pct'].cumsum()
    td = total.groupby('close_date')['Total'].last()
    chart_data = chart_data.join(td, how='outer').sort_index().ffill().fillna(0)

    # Calc MDD for total
    tc = chart_data['Total'] if 'Total' in chart_data.columns else pd.Series(dtype=float)
    tm = 0
    tms = tme = None
    tdd = pd.Series(dtype=float)
    if not tc.empty:
        start_idx = tc.index[0] - pd.Timedelta(days=1)
        cs0 = pd.concat([pd.Series([0.0], index=[start_idx]), tc])
        pk = cs0.cummax()
        dd_s = cs0 - pk
        tm = dd_s.min()
        if tm < 0:
            me = dd_s.idxmin()
            tms = cs0.loc[:me].idxmax()
            tme = me
            tdd = dd_s.iloc[1:]
        else:
            tm = 0
            tdd = pd.Series(0, index=tc.index)

    # Per-strategy MDD
    smdd = {}
    for s in strats_data:
        if s in chart_data.columns:
            sv_series = chart_data[s]
            if not sv_series.empty:
                si = sv_series.index[0] - pd.Timedelta(days=1)
                s0 = pd.concat([pd.Series([0.0], index=[si]), sv_series])
                spk = s0.cummax()
                sdd = s0 - spk
                smv = sdd.min()
                if smv < 0:
                    sme = sdd.idxmin()
                    sms = s0.loc[:sme].idxmax()
                    smdd[s] = {'mdd': smv, 'start': sms, 'end': sme, 'dd': sdd.iloc[1:]}
                else:
                    smdd[s] = {'mdd': 0, 'start': None, 'end': None, 'dd': pd.Series(0, index=sv_series.index)}

    # Summary metrics
    tp_sum = pnl_df['result_pct'].sum()
    ap = pnl_df['result_pct'].mean()
    nt = len(pnl_df)
    nw_profit = len(pnl_df[pnl_df['result_pct'] > 0])
    tw = (nw_profit / nt * 100) if nt > 0 else 0

    tc_ = '#5cc49a' if tp_sum > 0 else '#c46b7c' if tp_sum < 0 else '#4d5b72'
    mc_ = '#c46b7c' if tm < -5 else '#b8954a' if tm < 0 else '#4d5b72'
    wc_ = '#5cc49a' if tw >= 80 else '#b8954a' if tw > 0 else '#4d5b72'

    st.markdown(f'''<div class="rr-stats">
        <div class="rr-stat"><div class="s-label">Cumulative P&L</div><div class="s-value" style="color:{tc_}">{tp_sum:+.1f}%</div></div>
        <div class="rr-stat"><div class="s-label">Avg per Trade</div><div class="s-value" style="color:{tc_}">{ap:+.2f}%</div></div>
        <div class="rr-stat"><div class="s-label">Trades</div><div class="s-value" style="color:#8a9ab5">{nt}</div></div>
        <div class="rr-stat"><div class="s-label">Profit Rate</div><div class="s-value" style="color:{wc_}">{tw:.0f}%</div></div>
        <div class="rr-stat"><div class="s-label">Max Drawdown</div><div class="s-value" style="color:{mc_}">{tm:.1f}%</div></div>
    </div>''', unsafe_allow_html=True)

    if tms is not None and tme is not None:
        ms_s = tms.strftime('%Y-%m-%d') if hasattr(tms, 'strftime') else str(tms)
        me_s = tme.strftime('%Y-%m-%d') if hasattr(tme, 'strftime') else str(tme)
        st.markdown(f'<div class="rr-legend" style="color:#c46b7c">MDD Period: {ms_s} → {me_s} ({tm:.1f}%p)</div>', unsafe_allow_html=True)

    # Line chart: cumulative P&L
    st.markdown('<div class="rr-legend">Cumulative P&L by Strategy + Total</div>', unsafe_allow_html=True)
    cl = [sc.get(c, '#c9a96e') if c != 'Total' else '#c9a96e' for c in chart_data.columns]
    st.line_chart(chart_data, color=cl if cl else None)

    # Drawdown area chart
    st.markdown('<div class="rr-legend" style="margin-top:16px">Drawdown — 0% = Peak, Negative = Loss from Peak</div>', unsafe_allow_html=True)
    dd_c = pd.DataFrame()
    for s in strats_data:
        if s in smdd:
            dd_c[s] = smdd[s]['dd']
    if not tdd.empty:
        dd_c['Total'] = tdd
    dd_c = dd_c.sort_index().ffill().fillna(0)
    dcl = [sc.get(c, '#c9a96e') if c != 'Total' else '#c9a96e' for c in dd_c.columns]
    st.area_chart(dd_c, color=dcl if dcl else None)

    # Strategy risk summary table
    st.markdown('<div class="rr-legend" style="margin-top:16px">Strategy Risk Summary</div>', unsafe_allow_html=True)
    sh = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
    sh += '<th>Strategy</th><th>Trades</th><th>Win%</th><th>Cum P&L</th><th>Avg</th>'
    sh += '<th>Best</th><th>Worst</th><th style="color:#c46b7c">MDD</th><th>MDD Period</th>'
    sh += '</tr></thead><tbody>'

    for s in strats_data:
        sp = pnl_df[pnl_df['strategy'] == s]['result_pct']
        sn = len(sp)
        sw = len(sp[sp > 0])
        sr = (sw / sn * 100) if sn > 0 else 0
        st_ = sp.sum()
        sa = sp.mean()
        smx = sp.max()
        smn = sp.min()
        stc = '#5cc49a' if st_ > 0 else '#c46b7c' if st_ < 0 else '#4d5b72'
        swc = 'c-win' if sr >= 80 else 'c-partial' if sr > 0 else 'c-none'
        si = smdd.get(s, {})
        sm_val = si.get('mdd', 0)
        sms_v = si.get('start')
        sme_v = si.get('end')
        smc = '#c46b7c' if sm_val < -5 else '#b8954a' if sm_val < 0 else '#4d5b72'
        if sms_v and sme_v:
            msr = f'{sms_v.strftime("%m/%d") if hasattr(sms_v, "strftime") else str(sms_v)[:5]}→{sme_v.strftime("%m/%d") if hasattr(sme_v, "strftime") else str(sme_v)[:5]}'
        else:
            msr = '—'

        sh += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td><td>{sn}</td>'
        sh += f'<td class="{swc}">{sr:.0f}%</td>'
        sh += f'<td style="color:{stc};font-weight:600">{st_:+.1f}%</td>'
        sh += f'<td style="color:{stc}">{sa:+.2f}%</td>'
        sh += f'<td class="c-win">{smx:+.1f}%</td>'
        sh += f'<td class="c-loss">{smn:+.1f}%</td>'
        sh += f'<td style="color:{smc};font-weight:600">{sm_val:.1f}%</td>'
        sh += f'<td class="c-muted">{msr}</td></tr>'

    sh += '</tbody></table></div>'
    st.markdown(sh, unsafe_allow_html=True)


def render_analytics_suite(closed_pos, open_pos=None):
    """Comprehensive analytics suite with luxury HTML tables"""
    if closed_pos.empty or len(closed_pos) < 2:
        st.markdown('<div class="rr-empty">Not enough closed positions for analytics</div>', unsafe_allow_html=True)
        return

    rc = 'result_status' if 'result_status' in closed_pos.columns else 'status'
    strategies = ['A', 'B', 'C', 'D', 'E']

    an_cp = closed_pos.copy()
    an_cp['signal_date_dt'] = pd.to_datetime(an_cp.get('signal_date', ''), errors='coerce')
    an_cp['entry_date_dt'] = pd.to_datetime(an_cp.get('entry_date', ''), errors='coerce')
    an_cp['close_date_dt'] = pd.to_datetime(an_cp.get('close_date', ''), errors='coerce')
    an_cp['result_pct_f'] = pd.to_numeric(an_cp.get('result_pct', 0), errors='coerce').fillna(0)
    an_cp['days_held_f'] = pd.to_numeric(an_cp.get('days_held', 0), errors='coerce').fillna(0)
    an_cp['signal_price_f'] = pd.to_numeric(an_cp.get('signal_price', 0), errors='coerce').fillna(0)
    an_cp['entry_price_f'] = pd.to_numeric(an_cp.get('entry_price', 0), errors='coerce').fillna(0)
    an_cp['max_ach_f'] = pd.to_numeric(an_cp.get('max_achievement_pct', 0), errors='coerce').fillna(0)

    # ── 1) TIME-TO-TP ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">TP 도달 속도 (Time-to-TP)</div>', unsafe_allow_html=True)

    wins = an_cp[an_cp[rc] == 'WIN'].copy()
    if wins.empty:
        st.markdown('<div class="rr-empty">No WIN positions yet</div>', unsafe_allow_html=True)
    else:
        wins['ttp_days'] = wins['days_held_f']
        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
        html += '<th>Strategy</th><th>Wins</th><th>Avg Days</th><th>Median</th><th>Min</th><th>Max</th><th>1일 내 도달</th>'
        html += '</tr></thead><tbody>'
        for s in strategies:
            sw = wins[wins['strategy'] == s]
            if sw.empty:
                html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td><td>0</td><td colspan="5" class="c-muted">—</td></tr>'
                continue
            ttp = sw['ttp_days']
            avg_d = ttp.mean(); med_d = ttp.median(); min_d = ttp.min(); max_d = ttp.max()
            d1 = len(sw[ttp <= 1])
            d1_pct = (d1 / len(sw) * 100) if len(sw) > 0 else 0
            spd_color = '#5cc49a' if avg_d <= 2 else '#b8954a' if avg_d <= 5 else '#8a9ab5'
            html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td>'
            html += f'<td>{len(sw)}</td>'
            html += f'<td style="color:{spd_color};font-weight:700">{avg_d:.1f}일</td>'
            html += f'<td>{med_d:.0f}일</td><td>{min_d:.0f}일</td><td>{max_d:.0f}일</td>'
            html += f'<td style="color:#5cc49a">{d1} ({d1_pct:.0f}%)</td></tr>'
        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)
        st.markdown('<div class="rr-legend">빠를수록 자본 회전율이 높아져 복리 효과 극대화</div>', unsafe_allow_html=True)

    st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

    # ── 2) EXPECTED VALUE ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">기대값 (Expected Value per Trade)</div>', unsafe_allow_html=True)

    html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
    html += '<th>Strategy</th><th>Win Rate</th><th>Avg Win</th><th>Avg Loss</th><th>EV</th><th>Profit Factor</th>'
    html += '</tr></thead><tbody>'
    for s in strategies:
        sc_ = an_cp[an_cp['strategy'] == s]
        if sc_.empty:
            html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td><td colspan="5" class="c-muted">—</td></tr>'
            continue
        nw_ = len(sc_[sc_[rc] == 'WIN'])
        nl_ = len(sc_[sc_[rc].isin(['LOSS', 'EXPIRED'])])
        nc_ = nw_ + nl_
        wr_ = (nw_ / nc_ * 100) if nc_ > 0 else 0
        w_pnl = sc_[sc_[rc] == 'WIN']['result_pct_f']
        l_pnl = sc_[sc_[rc].isin(['LOSS', 'EXPIRED'])]['result_pct_f']
        avg_w = w_pnl.mean() if len(w_pnl) > 0 else 0
        avg_l = l_pnl.mean() if len(l_pnl) > 0 else 0
        ev = (wr_ / 100) * avg_w + (1 - wr_ / 100) * avg_l
        gross_w = w_pnl.sum() if len(w_pnl) > 0 else 0
        gross_l = abs(l_pnl.sum()) if len(l_pnl) > 0 else 0
        pf = (gross_w / gross_l) if gross_l > 0 else float('inf') if gross_w > 0 else 0
        ev_c = '#5cc49a' if ev > 0 else '#c46b7c' if ev < 0 else '#4d5b72'
        pf_c = '#5cc49a' if pf >= 2 else '#b8954a' if pf >= 1 else '#c46b7c'
        pf_s = f'{pf:.2f}' if pf != float('inf') else '∞'
        html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td>'
        html += f'<td>{wr_:.1f}%</td>'
        html += f'<td class="c-win">{avg_w:+.2f}%</td>'
        html += f'<td class="c-loss">{avg_l:+.2f}%</td>'
        html += f'<td style="color:{ev_c};font-weight:700;font-size:1.1em">{ev:+.2f}%</td>'
        html += f'<td style="color:{pf_c};font-weight:600">{pf_s}</td></tr>'
    html += '</tbody></table></div>'
    st.markdown(html, unsafe_allow_html=True)
    st.markdown('<div class="rr-legend">EV = (승률 × 평균수익) + (패률 × 평균손실) | Profit Factor = 총이익 / 총손실 (≥2.0 우수)</div>', unsafe_allow_html=True)

    st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

    # ── 3) DAY-OF-WEEK ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">요일별 신호 성과 (Day-of-Week)</div>', unsafe_allow_html=True)

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
            dw = len(d_data[d_data[rc] == 'WIN'])
            dl = len(d_data[d_data[rc].isin(['LOSS', 'EXPIRED'])])
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

    # ── 4) SLIPPAGE ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">슬리피지 (Signal vs Entry Price)</div>', unsafe_allow_html=True)

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
                html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td><td>0</td><td colspan="4" class="c-muted">—</td></tr>'
                continue
            sl_data = ss['slip_pct']
            avg_sl = sl_data.mean(); med_sl = sl_data.median()
            max_sl = sl_data.max()
            std_sl = sl_data.std() if len(sl_data) > 1 else 0.0
            sc_c = '#5cc49a' if abs(avg_sl) < 1 else '#b8954a' if abs(avg_sl) < 3 else '#c46b7c'
            html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td><td>{len(ss)}</td>'
            html += f'<td style="color:{sc_c};font-weight:600">{avg_sl:+.2f}%</td>'
            html += f'<td>{med_sl:+.2f}%</td>'
            html += f'<td class="c-loss">{max_sl:+.2f}%</td>'
            html += f'<td class="c-muted">{std_sl:.2f}%</td></tr>'
        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)
        st.markdown('<div class="rr-legend">양수 = 시그널가보다 비싸게 매수 (불리) | 음수 = 시그널가보다 싸게 매수 (유리)</div>', unsafe_allow_html=True)
        if abs(slip['slip_pct'].sum()) < 0.01:
            st.markdown('<div class="rr-legend" style="color:#4d5b72;font-style:italic">※ 현재 시스템은 시그널가 = 진입가 (애프터마켓 종가 매수). 실거래 시 슬리피지 발생 가능.</div>', unsafe_allow_html=True)

    st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

    # ── 5) WIN/LOSS STREAKS ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">연승·연패 분석 (Streak)</div>', unsafe_allow_html=True)

    streak_df = an_cp.dropna(subset=['close_date_dt']).sort_values('close_date_dt').copy()
    if len(streak_df) < 2:
        st.markdown('<div class="rr-empty">Not enough data</div>', unsafe_allow_html=True)
    else:
        streak_df['is_win'] = (streak_df[rc] == 'WIN').astype(int)

        def _calc_streaks(series):
            max_w = max_l = cur_w = cur_l = 0
            all_w = []; all_l = []
            for v in series:
                if v == 1:
                    cur_w += 1; cur_l = 0; max_w = max(max_w, cur_w)
                else:
                    cur_l += 1; cur_w = 0; max_l = max(max_l, cur_l)
                if cur_w > 0: all_w.append(cur_w)
                if cur_l > 0: all_l.append(cur_l)
            avg_w = sum(all_w) / len(all_w) if all_w else 0
            avg_l = sum(all_l) / len(all_l) if all_l else 0
            return max_w, max_l, avg_w, avg_l

        o_mw, o_ml, o_aw, o_al = _calc_streaks(streak_df['is_win'])
        html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
        html += '<th>Strategy</th><th>Max 연승</th><th>Max 연패</th><th>Avg 연승</th><th>Avg 연패</th>'
        html += '</tr></thead><tbody>'

        ml_c = '#c46b7c' if o_ml >= 5 else '#b8954a' if o_ml >= 3 else '#8a9ab5'
        html += f'<tr style="border-top:2px solid #9a7d4e"><td style="color:#c9a96e;font-weight:700">TOTAL</td>'
        html += f'<td class="c-win" style="font-weight:700;font-size:1.1em">{o_mw}</td>'
        html += f'<td style="color:{ml_c};font-weight:700;font-size:1.1em">{o_ml}</td>'
        html += f'<td class="c-win">{o_aw:.1f}</td><td class="c-loss">{o_al:.1f}</td></tr>'

        for s in strategies:
            s_streak = streak_df[streak_df['strategy'] == s]
            if len(s_streak) < 2:
                html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td><td colspan="4" class="c-muted">—</td></tr>'
                continue
            s_mw, s_ml, s_aw, s_al = _calc_streaks(s_streak['is_win'])
            sml_c = '#c46b7c' if s_ml >= 5 else '#b8954a' if s_ml >= 3 else '#8a9ab5'
            html += f'<tr><td>{stag(s)} {STRAT_KR.get(s, "")}</td>'
            html += f'<td class="c-win">{s_mw}</td><td style="color:{sml_c};font-weight:600">{s_ml}</td>'
            html += f'<td class="c-win">{s_aw:.1f}</td><td class="c-loss">{s_al:.1f}</td></tr>'
        html += '</tbody></table></div>'
        st.markdown(html, unsafe_allow_html=True)
        st.markdown('<div class="rr-legend">최대 연패가 크면 자금관리(켈리 기준 등) 재검토 필요 — 심리적 한계선 설정 참고</div>', unsafe_allow_html=True)

    st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

    # ── 6) NEAR-MISS ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">아쉬운 실패 (Near-Miss Analysis)</div>', unsafe_allow_html=True)

    non_wins = an_cp[an_cp[rc].isin(['LOSS', 'EXPIRED'])].copy()
    if non_wins.empty:
        st.markdown('<div class="rr-empty">No losses/expirations</div>', unsafe_allow_html=True)
    else:
        nm_80 = non_wins[non_wins['max_ach_f'] >= 80]
        nm_50 = non_wins[(non_wins['max_ach_f'] >= 50) & (non_wins['max_ach_f'] < 80)]
        nm_low = non_wins[non_wins['max_ach_f'] < 50]
        total_nw = len(non_wins)

        st.markdown(f'''<div class="rr-stats">
            <div class="rr-stat"><div class="s-label">Total Loss/Exp</div><div class="s-value" style="color:#c46b7c">{total_nw}</div></div>
            <div class="rr-stat"><div class="s-label">≥80% 도달 후 실패</div><div class="s-value" style="color:#b8954a">{len(nm_80)}</div></div>
            <div class="rr-stat"><div class="s-label">50~80% 도달</div><div class="s-value" style="color:#8a9ab5">{len(nm_50)}</div></div>
            <div class="rr-stat"><div class="s-label">&lt;50% (완전 실패)</div><div class="s-value" style="color:#c46b7c">{len(nm_low)}</div></div>
        </div>''', unsafe_allow_html=True)

        if not nm_80.empty:
            nm_rate = len(nm_80) / total_nw * 100
            st.markdown(f'<div class="rr-legend" style="color:#b8954a">Near-miss rate: {nm_rate:.0f}% — TP의 80% 이상 도달했지만 실패한 비율이 높으면 TP 하향 조정 검토</div>', unsafe_allow_html=True)

            html = '<div class="rr-table-wrap"><table class="rr-table"><thead><tr>'
            html += '<th>Strat</th><th>Ticker</th><th>Signal</th><th>Result</th><th>Max Ach%</th><th>Result P&L</th><th>Peak</th>'
            html += '</tr></thead><tbody>'
            for _, row in nm_80.sort_values('max_ach_f', ascending=False).head(20).iterrows():
                s_ = safe_str(row.get('strategy')); tk_ = safe_str(row.get('ticker'))
                sd_ = safe_str(row.get('signal_date')); res_ = safe_str(row.get(rc))
                ma_ = row['max_ach_f']; rp_ = row['result_pct_f']
                mx_ = safe_str(row.get('max_price'))
                html += f'<tr><td>{stag(s_)}</td><td style="font-weight:600">{tk_}</td><td>{sd_}</td>'
                html += f'<td>{result_badge(res_)}</td>'
                html += f'<td style="color:#b8954a;font-weight:700">{ma_:.0f}%</td>'
                html += f'<td class="c-loss">{rp_:+.1f}%</td>'
                html += f'<td>${mx_}</td></tr>'
            html += '</tbody></table></div>'
            st.markdown(html, unsafe_allow_html=True)

    st.markdown('<div class="rr-divider" style="margin:28px auto"></div>', unsafe_allow_html=True)

    # ── 7) CONCURRENT POSITIONS ──
    st.markdown('<div class="rr-legend" style="color:#c9a96e;font-size:0.95em;margin-bottom:12px">동시 포지션 수 (Concurrent Positions)</div>', unsafe_allow_html=True)

    pos_events = []
    for _, row in an_cp.iterrows():
        ed = row['entry_date_dt']
        cd = row['close_date_dt']
        if pd.isna(ed):
            continue
        pos_events.append({'open': ed, 'close': cd if pd.notna(cd) else pd.Timestamp.now(), 'strategy': safe_str(row.get('strategy'))})

    if open_pos is not None and not open_pos.empty and 'entry_date' in open_pos.columns:
        for _, row in open_pos.iterrows():
            ed = pd.to_datetime(row.get('entry_date', ''), errors='coerce')
            if pd.isna(ed):
                continue
            pos_events.append({'open': ed, 'close': pd.Timestamp.now(), 'strategy': safe_str(row.get('strategy'))})

    if not pos_events:
        st.markdown('<div class="rr-empty">No position data</div>', unsafe_allow_html=True)
    else:
        all_dates = set()
        for pe in pos_events:
            try:
                dr = pd.date_range(pe['open'], pe['close'], freq='B')
                all_dates.update(dr)
            except Exception:
                pass
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

            mc_c = '#c46b7c' if max_conc >= 10 else '#b8954a' if max_conc >= 5 else '#5cc49a'
            st.markdown(f'''<div class="rr-stats">
                <div class="rr-stat"><div class="s-label">Max Concurrent</div><div class="s-value" style="color:{mc_c}">{max_conc}</div></div>
                <div class="rr-stat"><div class="s-label">Avg Concurrent</div><div class="s-value" style="color:#8a9ab5">{avg_conc:.1f}</div></div>
                <div class="rr-stat"><div class="s-label">Peak Date</div><div class="s-value" style="color:#8a9ab5;font-size:0.8em">{max_date_s}</div></div>
            </div>''', unsafe_allow_html=True)

            st.markdown('<div class="rr-legend">동시 포지션 수 추이 — 자금 배분 및 리스크 노출 관리에 활용</div>', unsafe_allow_html=True)
            st.area_chart(conc_df, color=['#c9a96e'])
        else:
            st.markdown('<div class="rr-empty">No date range data</div>', unsafe_allow_html=True)


# ============================================================================
# PAGE LAYOUT
# ============================================================================

def main():
    """Main page layout"""
    # Page config
    st.set_page_config(
        page_title="Surge Scanner",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Inject custom CSS
    st.html(get_custom_css())

    # Hide streamlit default UI elements
    st.html("""
    <style>
        .stDeployButton { display: none; }
        header[data-testid="stHeader"] { display: none; }
        footer { display: none; }
        #MainMenu { display: none; }
    </style>
    """)

    # Page sections
    render_header()
    render_todays_signals()
    render_active_positions()
    render_recent_results()
    render_performance()

    # Footer
    st.html(f"""
    <div style="text-align: center; padding: 2rem 1rem; color: {COLOR_PALETTE['text_muted']}; font-size: 0.8rem; margin-top: 2rem; border-top: 1px solid {COLOR_PALETTE['border_subtle']};">
        Surge Scanner · Last updated {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')} KST
    </div>
    """)

    # ── Sidebar ──
    with st.sidebar:
        st.markdown(f'''<div style="text-align:center;padding:16px 0">
            <div style="font-family:'Cormorant Garamond',serif;font-size:1.2em;color:{COLOR_PALETTE['text_primary']};letter-spacing:0.1em">SURGE</div>
            <div style="font-family:'Cormorant Garamond',serif;font-size:0.9em;color:{COLOR_PALETTE['gold']};letter-spacing:0.15em">SCANNER</div>
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

        tracker_info = load_tracker_summary()
        if tracker_info:
            st.markdown(f"**Last tracked:** {tracker_info.get('last_tracked', 'N/A')}")
            st.markdown(f"Active: {tracker_info.get('open_count', 0)} | Pending: {tracker_info.get('pending_count', 0)} | Closed: {tracker_info.get('closed_count', 0)}")
        if st.button("Refresh"):
            st.cache_data.clear()
            st.rerun()
        st.divider()

        st.markdown("**Auto Refresh**")
        ar = st.toggle("Enable", value=False, key='ar')
        ri = st.select_slider("Interval", options=[1, 2, 3, 5, 10, 15, 30], value=5, key='ri')
        st.caption(f"{'Active' if ar else 'Paused'} — {ri}min")
        if ar:
            import streamlit.components.v1 as components
            components.html(f"""<script>setTimeout(function(){{ window.parent.location.reload(); }}, {ri*60*1000});</script>""", height=0)


if __name__ == "__main__":
    main()
