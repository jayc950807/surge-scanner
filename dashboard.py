"""
SURGE SCANNER DASHBOARD
US Stock Surge Scanner - Luxury Dashboard Interface
"If Rolls-Royce made a trading app"
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import traceback

# Try to import KST from shared_config, fallback to local definition
try:
    from shared_config import KST
except ImportError:
    KST = timezone(timedelta(hours=9))

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

STRAT_NAMES = {
    'A': '급락반등',
    'B': '고수익',
    'C': '과매도',
    'D': '초저가',
    'E': '속반등'
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
        # Lower distance to SL = higher risk
        if sl > 0 and entry > 0 and current > sl:
            sl_distance_pct = ((current - sl) / entry) * 100
            # If within 2% of SL, get high score
            score += max(0, 40 - (sl_distance_pct * 20))

        # Factor 2: Time pressure (0-30 points)
        # More time used = higher risk
        if max_hold > 0 and days >= 0:
            time_used_ratio = days / max_hold
            score += min(30, time_used_ratio * 30)

        # Factor 3: Negative performance (0-30 points)
        # More negative = higher risk
        if change_pct < 0:
            score += min(30, abs(change_pct) * 1.5)

    except Exception:
        pass

    return score


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
    """Load closed position history"""
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
                return df.head(20)  # Last 20 results
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


def render_performance_stats():
    """Render performance statistics in expander"""
    with st.expander("📊 Performance Statistics", expanded=False):
        tracker = load_tracker_summary()
        closed_pos_df = load_closed_positions()

        try:
            total_closed = safe_float(tracker.get('closed_count', 0))
            wins = safe_float(tracker.get('win_count', 0))
            losses = safe_float(tracker.get('loss_count', 0))
            expired = safe_float(tracker.get('expired_count', 0))

            win_rate = (wins / total_closed * 100) if total_closed > 0 else 0

            # Calculate average return
            avg_return = 0.0
            if len(closed_pos_df) > 0:
                result_pcts = [safe_float(v) for v in closed_pos_df.get('result_pct', [])]
                result_pcts = [v for v in result_pcts if v != 0]
                avg_return = sum(result_pcts) / len(result_pcts) if len(result_pcts) > 0 else 0

            # Stats grid
            stats_html = f"""
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="stat-label">Total Trades</div>
                    <div class="stat-value">{int(total_closed)}</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Win Rate</div>
                    <div class="stat-value">{win_rate:.1f}%</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Avg Return</div>
                    <div class="stat-value">{format_percent(avg_return, 2)}</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Wins</div>
                    <div class="stat-value">{int(wins)}</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Losses</div>
                    <div class="stat-value">{int(losses)}</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Expired</div>
                    <div class="stat-value">{int(expired)}</div>
                </div>
            </div>
            """
            st.html(stats_html)

            # Strategy breakdown
            st.write("")
            st.subheader("By Strategy")

            strat_breakdown_html = '<div class="strategy-breakdown">'

            for strategy in ['A', 'B', 'C', 'D', 'E']:
                strat_closed = closed_pos_df[closed_pos_df['strategy'] == strategy]

                if len(strat_closed) > 0:
                    strat_wins = len(strat_closed[strat_closed['result_status'] == 'WIN'])
                    strat_win_rate = (strat_wins / len(strat_closed)) * 100

                    strat_pcts = [safe_float(v) for v in strat_closed.get('result_pct', [])]
                    strat_pcts = [v for v in strat_pcts if v != 0]
                    strat_avg_return = sum(strat_pcts) / len(strat_pcts) if len(strat_pcts) > 0 else 0

                    color = get_strategy_color(strategy)
                    strat_name = get_strategy_name(strategy)

                    strat_breakdown_html += f"""
                    <div class="strat-row">
                        <span class="signal-badge" style="background-color: {color}">{strategy}</span>
                        <span class="strat-name">{strat_name}</span>
                        <span class="strat-stat">
                            {int(len(strat_closed))} trades · {strat_win_rate:.0f}% win · {format_percent(strat_avg_return, 1)}
                        </span>
                    </div>
                    """

            strat_breakdown_html += '</div>'
            st.html(strat_breakdown_html)

        except Exception as e:
            st.error(f"Error rendering performance stats: {e}")


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
    render_performance_stats()

    # Footer
    st.html(f"""
    <div style="text-align: center; padding: 2rem 1rem; color: {COLOR_PALETTE['text_muted']}; font-size: 0.8rem; margin-top: 2rem; border-top: 1px solid {COLOR_PALETTE['border_subtle']};">
        Surge Scanner · Last updated {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')} KST
    </div>
    """)


if __name__ == "__main__":
    main()
