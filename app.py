#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Streamlit Dashboard
  Strategy A (+5%) + Strategy B (+15%) 실시간 대시보드
================================================================================
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timedelta

# ─── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Surge Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Custom CSS (모바일 반응형) ───────────────────────────────────────────────

st.markdown("""
<style>
    .main .block-container { padding-top: 1rem; max-width: 1200px; }
    .stMetric > div { background: #f8f9fa; border-radius: 8px; padding: 12px; }
    .no-signal {
        text-align: center; padding: 40px; color: #888;
        font-size: 1.2em; background: #f8f9fa; border-radius: 12px;
    }
    @media (max-width: 768px) {
        .main .block-container { padding: 0.5rem; }
    }
</style>
""", unsafe_allow_html=True)


# ─── Data Loading ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_latest_scan():
    json_path = "data/latest_scan.json"
    if os.path.exists(json_path):
        with open(json_path, 'r') as f:
            return json.load(f)
    return None


@st.cache_data(ttl=300)
def load_today_signals():
    today = datetime.now().strftime('%Y-%m-%d')
    path = f"data/signal_{today}.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        return df if len(df) > 0 else pd.DataFrame()
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_history():
    path = "data/history.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()


# ─── Header ───────────────────────────────────────────────────────────────────

st.title("📈 US Stock Surge Scanner")

scan_info = load_latest_scan()
if scan_info:
    st.caption(f"마지막 스캔: {scan_info.get('scan_time', 'N/A')} | "
               f"Strategy A: {scan_info.get('strategy_a_count', 0)}건 | "
               f"Strategy B: {scan_info.get('strategy_b_count', 0)}건")
else:
    st.caption("아직 스캔 결과가 없습니다. scanner.py를 실행하세요.")

st.divider()

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_a, tab_b, tab_history = st.tabs([
    "🟢 Strategy A (+5%)",
    "🔵 Strategy B (+15%)",
    "📊 히스토리",
])

today_signals = load_today_signals()
history = load_history()

# ─── Tab: Strategy A ─────────────────────────────────────────────────────────

with tab_a:
    st.subheader("Strategy A — Weekly Signal (+5% in 5 Days)")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **매수 조건** (5개 모두 충족):
        - 일중 변동폭 > 20%
        - 3일 수익률 < -15%
        - 연속 하락일 > 5일
        - 5일 최저가 대비 < 5%
        - RSI(7) < 20
        """)
    with col2:
        st.markdown("""
        **청산 규칙**:
        - 익절: +5%
        - 손절: -20%
        - 트레일링: -3%
        - 타임아웃: 5일

        **백테스트**: 승률 90.1% (262건), 누적 +515%
        """)

    st.divider()

    if not today_signals.empty and 'strategy' in today_signals.columns:
        sig_a = today_signals[today_signals['strategy'] == 'A']
    else:
        sig_a = pd.DataFrame()

    if not sig_a.empty:
        st.success(f"오늘 신호: {len(sig_a)}건")
        display_cols_a = ['ticker', 'price', 'rsi7', 'intraday', 'ret3d',
                         'consec_down', 'dist_low5', 'tp_price', 'sl_price']
        available_cols = [c for c in display_cols_a if c in sig_a.columns]
        st.dataframe(
            sig_a[available_cols].reset_index(drop=True),
            use_container_width=True,
            column_config={
                "ticker": st.column_config.TextColumn("종목", width="small"),
                "price": st.column_config.NumberColumn("종가", format="$%.2f"),
                "rsi7": st.column_config.NumberColumn("RSI7", format="%.1f"),
                "intraday": st.column_config.NumberColumn("일중%", format="%.1f%%"),
                "ret3d": st.column_config.NumberColumn("3일%", format="%.1f%%"),
                "consec_down": st.column_config.NumberColumn("연속↓", format="%d일"),
                "dist_low5": st.column_config.NumberColumn("Low5%", format="%.1f%%"),
                "tp_price": st.column_config.NumberColumn("익절가", format="$%.2f"),
                "sl_price": st.column_config.NumberColumn("손절가", format="$%.2f"),
            },
        )
        st.info("※ 다음 거래일 시장가 매수 (D+1 Open) → -20% 손절 주문 → 수익 시 -3% 트레일링 → 목표 +5% 또는 5일 타임아웃")
    else:
        st.markdown('<div class="no-signal">오늘 Strategy A 신호 없음</div>', unsafe_allow_html=True)

    if not history.empty and 'strategy' in history.columns:
        hist_a = history[history['strategy'] == 'A']
        if not hist_a.empty:
            cutoff = datetime.now() - timedelta(days=30)
            recent_a = hist_a[hist_a['date'] >= cutoff]
            if not recent_a.empty:
                st.divider()
                st.subheader("최근 30일 신호")
                st.dataframe(recent_a.sort_values('date', ascending=False).reset_index(drop=True),
                           use_container_width=True)


# ─── Tab: Strategy B ─────────────────────────────────────────────────────────

with tab_b:
    st.subheader("Strategy B — High-Gain (+15%)")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **매수 조건** (6개 모두 충족):
        - RSI(7) < 20
        - RSI(14) < 35
        - ATR 배율 > 3 (SMA5/SMA20)
        - 일중 변동폭 > 15%
        - MA20 대비 <= -25%
        - 매출 성장률 > 0%
        """)
    with col2:
        st.markdown("""
        **청산 규칙**:
        - 익절: +15% 지정가
        - 손절: -20%
        - 타임아웃: 10일 종가청산

        **백테스트**: 승률 90.3% (31건/28적중)
        """)

    st.divider()

    if not today_signals.empty and 'strategy' in today_signals.columns:
        sig_b = today_signals[today_signals['strategy'] == 'B']
    else:
        sig_b = pd.DataFrame()

    if not sig_b.empty:
        st.success(f"오늘 신호: {len(sig_b)}건")
        display_cols_b = ['ticker', 'price', 'rsi7', 'rsi14', 'atr_ratio',
                         'intra_pct', 'ma20_pos', 'rev_growth', 'tp_price', 'sl_price']
        available_cols = [c for c in display_cols_b if c in sig_b.columns]
        st.dataframe(
            sig_b[available_cols].reset_index(drop=True),
            use_container_width=True,
            column_config={
                "ticker": st.column_config.TextColumn("종목", width="small"),
                "price": st.column_config.NumberColumn("종가", format="$%.2f"),
                "rsi7": st.column_config.NumberColumn("RSI7", format="%.1f"),
                "rsi14": st.column_config.NumberColumn("RSI14", format="%.1f"),
                "atr_ratio": st.column_config.NumberColumn("ATR배율", format="%.2f"),
                "intra_pct": st.column_config.NumberColumn("일중%", format="%.1f%%"),
                "ma20_pos": st.column_config.NumberColumn("MA20%", format="%.1f%%"),
                "rev_growth": st.column_config.NumberColumn("매출성장", format="%.1f%%"),
                "tp_price": st.column_config.NumberColumn("매도가", format="$%.2f"),
                "sl_price": st.column_config.NumberColumn("손절가", format="$%.2f"),
            },
        )
        st.info("※ 매수 즉시 +15% 지정가 매도 + -20% 손절 주문 동시 설정. 둘 다 미체결 시 10거래일 후 종가 청산")
    else:
        st.markdown('<div class="no-signal">오늘 Strategy B 신호 없음</div>', unsafe_allow_html=True)

    if not history.empty and 'strategy' in history.columns:
        hist_b = history[history['strategy'] == 'B']
        if not hist_b.empty:
            cutoff = datetime.now() - timedelta(days=30)
            recent_b = hist_b[hist_b['date'] >= cutoff]
            if not recent_b.empty:
                st.divider()
                st.subheader("최근 30일 신호")
                st.dataframe(recent_b.sort_values('date', ascending=False).reset_index(drop=True),
                           use_container_width=True)


# ─── Tab: History ─────────────────────────────────────────────────────────────

with tab_history:
    st.subheader("📊 신호 히스토리")

    if not history.empty:
        hist_monthly = history.copy()
        hist_monthly['month'] = hist_monthly['date'].dt.to_period('M').astype(str)

        if 'strategy' in hist_monthly.columns:
            monthly_counts = hist_monthly.groupby(['month', 'strategy']).size().unstack(fill_value=0)
        else:
            monthly_counts = hist_monthly.groupby('month').size().to_frame('signals')

        st.bar_chart(monthly_counts)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("전체 신호", f"{len(history)}건")
        with col2:
            if 'strategy' in history.columns:
                st.metric("Strategy A", f"{len(history[history['strategy']=='A'])}건")
        with col3:
            if 'strategy' in history.columns:
                st.metric("Strategy B", f"{len(history[history['strategy']=='B'])}건")

        st.divider()
        st.dataframe(
            history.sort_values('date', ascending=False).reset_index(drop=True),
            use_container_width=True,
        )
    else:
        st.markdown('<div class="no-signal">아직 히스토리 데이터가 없습니다</div>',
                   unsafe_allow_html=True)


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("정보")
    st.markdown("""
    **US Stock Surge Scanner**

    매일 미국 장 마감 후 (KST 06:00) 자동 스캔하여
    급락 후 반등 가능성이 높은 종목을 포착합니다.

    **Strategy A** — 단기 +5% 목표
    - 승률 90.1%, 누적 +515%
    - 5일 이내 청산

    **Strategy B** — 중기 +15% 목표
    - 승률 90.3%
    - 10일 이내 청산

    ---
    GitHub Actions로 평일 자동 실행
    """)

    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()
