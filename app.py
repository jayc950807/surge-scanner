#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Streamlit Dashboard
  Strategy A/B/C/D/E 대시보드
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
# ─── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main .block-container { padding-top: 1rem; max-width: 1400px; }
    .stMetric > div { background: #1e1e2e; border-radius: 8px; padding: 12px; }
    .no-signal {
        text-align: center; padding: 40px; color: #888;
        font-size: 1.1em; background: #1a1a2e; border-radius: 12px;
    }
    /* ── 히스토리 매트릭스 테이블 ── */
    .matrix-table {
        width: 100%; border-collapse: collapse; font-size: 0.88em;
        background: #0e1117; border-radius: 8px; overflow: hidden;
        box-shadow: 0 1px 4px rgba(0,0,0,0.3);
    }
    .matrix-table th {
        background: #1a1a2e; color: #e0e0e0; padding: 10px 14px;
        text-align: center; font-weight: 600; font-size: 0.85em;
        border-bottom: 2px solid #2a2a4e;
    }
    .matrix-table th.strat-col {
        background: #16213e; text-align: left; width: 80px;
    }
    .matrix-table td {
        padding: 10px 14px; text-align: center; border-bottom: 1px solid #1e2030;
        font-weight: 500; font-variant-numeric: tabular-nums;
        color: #d0d0d0;
    }
    .matrix-table tr:hover td { background: #1a1a30; }
    .matrix-table .strat-label {
        font-weight: 700; text-align: left; background: #12121e; color: #e0e0e0;
    }
    /* 셀 색상 */
    .cell-win { color: #00e676; font-weight: 700; }
    .cell-none { color: #555; }
    .cell-partial { color: #ffab40; font-weight: 600; }
    .cell-zero { color: #666; }
    /* 전략 뱃지 */
    .strat-badge {
        display: inline-block; padding: 2px 10px; border-radius: 12px;
        font-weight: 700; font-size: 0.82em; color: #fff; min-width: 50px; text-align: center;
    }
    .strat-A { background: #00c853; } .strat-B { background: #2979ff; }
    .strat-C { background: #ff9100; } .strat-D { background: #ff1744; }
    .strat-E { background: #7c4dff; }
    /* 요약 카드 */
    .summary-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px; padding: 20px; color: #fff; text-align: center;
        border: 1px solid #2a2a4e;
    }
    .summary-card .value { font-size: 2em; font-weight: 800; line-height: 1.2; }
    .summary-card .label { font-size: 0.82em; color: #a0a0c0; margin-top: 4px; }
    /* 포지션 테이블 */
    .pos-table {
        width: 100%; border-collapse: collapse; font-size: 0.88em;
        background: #0e1117; border-radius: 8px; overflow: hidden;
        box-shadow: 0 1px 4px rgba(0,0,0,0.3);
    }
    .pos-table th {
        background: #1a1a2e; color: #e0e0e0; padding: 10px 12px;
        text-align: left; font-weight: 600; font-size: 0.82em;
    }
    .pos-table td {
        padding: 10px 12px; border-bottom: 1px solid #1e2030;
        font-weight: 500; color: #d0d0d0;
    }
    .pos-table tr:hover td { background: #1a1a30; }
    .status-pending { color: #9e9e9e; font-weight: 600; }
    .status-open { color: #42a5f5; font-weight: 700; }
    .status-win { color: #00e676; font-weight: 700; }
    .status-loss { color: #ff5252; font-weight: 700; }
    .status-expired { color: #ffab40; font-weight: 700; }
    .price-none { color: #555; font-style: italic; }
    /* 티커별 카드 */
    .ticker-card {
        background: #12121e; border: 1px solid #2a2a4e; border-radius: 10px;
        padding: 16px; margin-bottom: 12px;
    }
    .ticker-card-header {
        display: flex; justify-content: space-between; align-items: center;
        margin-bottom: 10px; padding-bottom: 8px; border-bottom: 1px solid #1e2030;
    }
    .ticker-name { font-size: 1.2em; font-weight: 800; color: #ffffff; }
    .ticker-summary { font-size: 0.85em; color: #a0a0c0; }
    /* 진행바 (달성률) */
    .achievement-bar {
        height: 10px; border-radius: 5px; background: #1e2030; overflow: hidden;
        display: inline-block; width: 80px; vertical-align: middle;
    }
    .achievement-fill { height: 100%; border-radius: 5px; }
    .achievement-text { font-size: 0.82em; font-weight: 700; margin-left: 6px; }
    /* 히스토리 설명 텍스트 */
    .history-desc { color: #a0a0c0; font-size: 0.85em; margin-bottom: 12px; }
    @media (max-width: 768px) {
        .main .block-container { padding: 0.5rem; }
        .matrix-table th, .matrix-table td { padding: 6px 8px; font-size: 0.8em; }
        .pos-table th, .pos-table td { padding: 6px 8px; font-size: 0.8em; }
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
@st.cache_data(ttl=300)
def load_tracker_summary():
    path = "data/tracker_summary.json"
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return None
# ─── Header ───────────────────────────────────────────────────────────────────
st.title("📈 US Stock Surge Scanner")
scan_info = load_latest_scan()
tracker_info = load_tracker_summary()
header_parts = []
if scan_info:
    header_parts.append(f"마지막 스캔: {scan_info.get('scan_time', 'N/A')}")
    header_parts.append(
        f"A: {scan_info.get('strategy_a_count', 0)}건 | "
        f"B: {scan_info.get('strategy_b_count', 0)}건 | "
        f"C: {scan_info.get('strategy_c_count', 0)}건 | "
        f"D: {scan_info.get('strategy_d_count', 0)}건 | "
        f"E: {scan_info.get('strategy_e_count', 0)}건"
    )
if tracker_info:
    header_parts.append(
        f"추적: OPEN {tracker_info.get('open_count', 0)}건 | "
        f"WIN {tracker_info.get('win_count', 0)}건 | "
        f"LOSS {tracker_info.get('loss_count', 0)}건"
    )
if header_parts:
    st.caption(" | ".join(header_parts))
else:
    st.caption("아직 스캔 결과가 없습니다. scanner.py를 실행하세요.")
st.divider()
# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_a, tab_b, tab_c, tab_d, tab_e, tab_history = st.tabs([
    "🟢 A — 급락 반등 +5%",
    "🔵 B — 고수익 +15%",
    "🟠 C — 과매도 반등 +5%",
    "🔴 D — 초저가 폭락 +20%",
    "🟣 E — 급락 속반등 +10%",
    "📊 히스토리",
])
today_signals = load_today_signals()
history = load_history()
# ─── Tab: Strategy A ─────────────────────────────────────────────────────────
with tab_a:
    st.subheader("🟢 전략 A — 급락 반등 (+5% / 5일)")
    st.markdown("""
    📌 **이런 종목을 찾습니다:**
    5일 넘게 연속 하락하면서 3일간 -15% 이상 빠지고,
    장중 위아래로 20% 이상 흔들리며,
    RSI가 20 이하로 극단적 과매도 상태이고,
    현재가가 5일 최저가와 거의 같은 바닥권 종목

    💰 **매매 방법:**
    매수: 다음 거래일 시장가 (D+1 시가) ─
    익절: +5% 도달 시 즉시 매도 ─
    손절: -20% ─
    트레일링: 고점 대비 -3% 하락 시 매도 ─
    보유: 최대 5일, 미도달 시 종가 청산

    📊 **백테스트 (5년, 262건):** 승률 90.1% | 누적 +515%
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
    st.subheader("🔵 전략 B — 고수익 폭락 반등 (+15% / 10일)")
    st.markdown("""
    📌 **이런 종목을 찾습니다:**
    RSI 7일·14일 모두 극단적 과매도 구간이면서,
    최근 변동성이 평소의 3배 이상으로 폭발하고,
    20일 이동평균 대비 -25% 이상 괴리가 벌어졌지만,
    매출은 여전히 성장 중인 펀더멘탈 있는 종목

    💰 **매매 방법:**
    매수: 다음 거래일 시장가 (D+1 시가) ─
    익절: +15% 지정가 매도 ─
    손절: -20% ─
    보유: 최대 10일, 미체결 시 종가 청산

    📊 **백테스트 (5년, 31건):** 승률 90.3% | 28건 적중
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
# ─── Tab: Strategy C ─────────────────────────────────────────────────────────
with tab_c:
    st.subheader("🟠 전략 C — 과매도 급락 반등 (+5% / 5일)")
    st.markdown("""
    📌 **이런 종목을 찾습니다:**
    최근 4일 이상 연속 하락하면서
    오늘 하루에만 -8% 이상 급락하고,
    장중 위아래로 20% 이상 흔들리며,
    RSI가 30 이하로 극단적 과매도 상태이고,
    현재가가 5일 최저가와 거의 같은 바닥권 종목

    💰 **매매 방법:**
    매수: 다음 거래일 시장가 (D+1 시가) ─
    익절: +5% 도달 시 즉시 매도 ─
    손절: -20% ─
    보유: 최대 5일, 미도달 시 종가 청산

    📊 **백테스트 (5년, 624건):** 승률 86.9% | 건당 평균 +3.8%
    """)
    st.divider()
    if not today_signals.empty and 'strategy' in today_signals.columns:
        sig_c = today_signals[today_signals['strategy'] == 'C']
    else:
        sig_c = pd.DataFrame()
    if not sig_c.empty:
        st.success(f"오늘 신호: {len(sig_c)}건")
        display_cols_c = ['ticker', 'price', 'rsi7', 'intraday', 'ret1d',
                         'consec_down', 'dist_low5', 'tp_price', 'sl_price']
        available_cols = [c for c in display_cols_c if c in sig_c.columns]
        st.dataframe(
            sig_c[available_cols].reset_index(drop=True),
            use_container_width=True,
            column_config={
                "ticker": st.column_config.TextColumn("종목", width="small"),
                "price": st.column_config.NumberColumn("종가", format="$%.2f"),
                "rsi7": st.column_config.NumberColumn("RSI7", format="%.1f"),
                "intraday": st.column_config.NumberColumn("일중%", format="%.1f%%"),
                "ret1d": st.column_config.NumberColumn("1일%", format="%.1f%%"),
                "consec_down": st.column_config.NumberColumn("연속↓", format="%d일"),
                "dist_low5": st.column_config.NumberColumn("Low5%", format="%.2f%%"),
                "tp_price": st.column_config.NumberColumn("익절가", format="$%.2f"),
                "sl_price": st.column_config.NumberColumn("손절가", format="$%.2f"),
            },
        )
        st.info("※ 다음 거래일 시장가 매수 (D+1 Open) → -20% 손절 주문 → 목표 +5% 또는 5일 타임아웃")
    else:
        st.markdown('<div class="no-signal">오늘 Strategy C 신호 없음</div>', unsafe_allow_html=True)
    if not history.empty and 'strategy' in history.columns:
        hist_c = history[history['strategy'] == 'C']
        if not hist_c.empty:
            cutoff = datetime.now() - timedelta(days=30)
            recent_c = hist_c[hist_c['date'] >= cutoff]
            if not recent_c.empty:
                st.divider()
                st.subheader("최근 30일 신호")
                st.dataframe(recent_c.sort_values('date', ascending=False).reset_index(drop=True),
                           use_container_width=True)
# ─── Tab: Strategy D ─────────────────────────────────────────────────────────
with tab_d:
    st.subheader("🔴 전략 D — 초저가 폭락 반등 (+20% / 30일)")
    st.markdown("""
    📌 **이런 종목을 찾습니다:**
    $3 이하 초저가주가 5일간 -40% 이상 대폭락하면서,
    장중 변동폭이 30% 이상으로 극단적으로 흔들리고,
    RSI(14)가 25 이하로 완전히 바닥을 찍은 종목

    💰 **매매 방법:**
    매수: 다음 거래일 시장가 (D+1 시가) ─
    익절: +20% 도달 시 즉시 매도 (중간값 2일 만에 도달) ─
    손절: 없음 ─
    보유: 최대 30일, 미도달 시 종가 청산

    📊 **백테스트 (5년, 130건):** 승률 97.7% | 건당 평균 +18.9%
    """)
    st.divider()
    if not today_signals.empty and 'strategy' in today_signals.columns:
        sig_d = today_signals[today_signals['strategy'] == 'D']
    else:
        sig_d = pd.DataFrame()
    if not sig_d.empty:
        st.success(f"오늘 신호: {len(sig_d)}건")
        display_cols_d = ['ticker', 'price', 'rsi14', 'intraday', 'ret5d',
                         'tp_price', 'hold_days']
        available_cols = [c for c in display_cols_d if c in sig_d.columns]
        st.dataframe(
            sig_d[available_cols].reset_index(drop=True),
            use_container_width=True,
            column_config={
                "ticker": st.column_config.TextColumn("종목", width="small"),
                "price": st.column_config.NumberColumn("종가", format="$%.2f"),
                "rsi14": st.column_config.NumberColumn("RSI14", format="%.1f"),
                "intraday": st.column_config.NumberColumn("일중%", format="%.1f%%"),
                "ret5d": st.column_config.NumberColumn("5일%", format="%.1f%%"),
                "tp_price": st.column_config.NumberColumn("익절가", format="$%.2f"),
                "hold_days": st.column_config.NumberColumn("보유일", format="%d일"),
            },
        )
        st.info("※ 다음 거래일 시장가 매수 (D+1 Open) → +20% 익절 지정가 설정 → 미체결 시 30일 후 종가 청산. 손절 없음")
    else:
        st.markdown('<div class="no-signal">오늘 Strategy D 신호 없음</div>', unsafe_allow_html=True)
    if not history.empty and 'strategy' in history.columns:
        hist_d = history[history['strategy'] == 'D']
        if not hist_d.empty:
            cutoff = datetime.now() - timedelta(days=30)
            recent_d = hist_d[hist_d['date'] >= cutoff]
            if not recent_d.empty:
                st.divider()
                st.subheader("최근 30일 신호")
                st.dataframe(recent_d.sort_values('date', ascending=False).reset_index(drop=True),
                           use_container_width=True)
# ─── Tab: Strategy E ─────────────────────────────────────────────────────────
with tab_e:
    st.subheader("🟣 전략 E — 급락 속반등 (+10% / 최대 30일)")
    st.markdown("""
    📌 **이런 종목을 찾습니다:**
    $3~$10 저가주가 5일간 -25% 이상 급락하면서,
    5일 이상 연속 하락하고,
    장중 변동폭이 20% 이상으로 크게 흔들리며,
    평균 거래량 20만주 이상으로 유동성이 확보된 종목

    💰 **매매 방법:**
    매수: 다음 거래일 시장가 (D+1 시가) ─
    익절: +10% 도달 시 즉시 매도 (중간값 2일 만에 도달) ─
    손절: 없음 ─
    보유: 최대 30일, 미도달 시 종가 청산

    📊 **백테스트 (5년, 300건):** 승률 91.0% | 건당 평균 max +104.9%

    ⏱ **보유기간별 도달률:**
    2일 보유: 53.0% (159/300건) ─
    5일 보유: 68.3% (205/300건) ─
    10일 보유: 78.7% (236/300건) ─
    30일 보유: 91.0% (273/300건)
    """)
    st.divider()
    if not today_signals.empty and 'strategy' in today_signals.columns:
        sig_e = today_signals[today_signals['strategy'] == 'E']
    else:
        sig_e = pd.DataFrame()
    if not sig_e.empty:
        st.success(f"오늘 신호: {len(sig_e)}건")
        display_cols_e = ['ticker', 'price', 'ret5d', 'intraday', 'consec_down',
                         'vol_avg', 'tp_price', 'hold_days']
        available_cols = [c for c in display_cols_e if c in sig_e.columns]
        st.dataframe(
            sig_e[available_cols].reset_index(drop=True),
            use_container_width=True,
            column_config={
                "ticker": st.column_config.TextColumn("종목", width="small"),
                "price": st.column_config.NumberColumn("종가", format="$%.2f"),
                "ret5d": st.column_config.NumberColumn("5일%", format="%.1f%%"),
                "intraday": st.column_config.NumberColumn("일중%", format="%.1f%%"),
                "consec_down": st.column_config.NumberColumn("연속↓", format="%d일"),
                "vol_avg": st.column_config.NumberColumn("평균거래량", format="%d"),
                "tp_price": st.column_config.NumberColumn("익절가", format="$%.2f"),
                "hold_days": st.column_config.NumberColumn("보유일", format="%d일"),
            },
        )
        st.info("※ 다음 거래일 시장가 매수 (D+1 Open) → +10% 익절 지정가 설정 → 미체결 시 30일 후 종가 청산. 손절 없음")
    else:
        st.markdown('<div class="no-signal">오늘 Strategy E 신호 없음</div>', unsafe_allow_html=True)
    if not history.empty and 'strategy' in history.columns:
        hist_e = history[history['strategy'] == 'E']
        if not hist_e.empty:
            cutoff = datetime.now() - timedelta(days=30)
            recent_e = hist_e[hist_e['date'] >= cutoff]
            if not recent_e.empty:
                st.divider()
                st.subheader("최근 30일 신호")
                st.dataframe(recent_e.sort_values('date', ascending=False).reset_index(drop=True),
                           use_container_width=True)
# ─── Data Loading: Positions ─────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_open_positions():
    path = "data/open_positions.csv"
    if os.path.exists(path):
        df = pd.read_csv(path, dtype=str)
        return df if len(df) > 0 else pd.DataFrame()
    return pd.DataFrame()
@st.cache_data(ttl=300)
def load_closed_positions():
    path = "data/closed_positions.csv"
    if os.path.exists(path):
        df = pd.read_csv(path, dtype=str)
        return df if len(df) > 0 else pd.DataFrame()
    return pd.DataFrame()
# ─── Helper: HTML 렌더링 함수들 ──────────────────────────────────────────────

STRAT_NAMES = {'A': '급락반등 +5%', 'B': '고수익 +15%', 'C': '과매도 +5%', 'D': '초저가 +20%', 'E': '속반등 +10%'}
STRAT_TP = {'A': '+5%', 'B': '+15%', 'C': '+5%', 'D': '+20%', 'E': '+10%'}
STRAT_BT_WR = {'A': '90.1%', 'B': '90.3%', 'C': '86.9%', 'D': '97.7%', 'E': '91.0%'}

def safe_str(val, fallback='—'):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN'):
        return fallback
    return str(val)

def safe_float(val, fallback=0):
    try:
        v = float(val)
        return v if not pd.isna(v) else fallback
    except:
        return fallback

def strat_badge(s):
    return f'<span class="strat-badge strat-{s}">{s}</span>'

def cell_html(ach, det):
    if det == 0:
        return '<span class="cell-none">—</span>'
    if ach == det:
        return f'<span class="cell-win">{ach}/{det}</span>'
    if ach == 0:
        return f'<span class="cell-partial">0/{det}</span>'
    return f'<span class="cell-partial">{ach}/{det}</span>'

def render_summary_card(value, label, color='#fff'):
    return f'''<div class="summary-card">
        <div class="value" style="color:{color}">{value}</div>
        <div class="label">{label}</div>
    </div>'''


# ─── Tab: History ─────────────────────────────────────────────────────────────
with tab_history:
    open_pos = load_open_positions()
    closed_pos = load_closed_positions()

    # ── 데이터 준비 ──
    all_records = pd.DataFrame()
    frames = []
    if not open_pos.empty and 'strategy' in open_pos.columns:
        tmp_open = open_pos[['strategy', 'signal_date', 'status']].copy()
        tmp_open.rename(columns={'status': 'result'}, inplace=True)
        frames.append(tmp_open)
    if not closed_pos.empty and 'strategy' in closed_pos.columns:
        result_col = 'result_status' if 'result_status' in closed_pos.columns else 'status'
        tmp_closed = closed_pos[['strategy', 'signal_date', result_col]].copy()
        tmp_closed.rename(columns={result_col: 'result'}, inplace=True)
        frames.append(tmp_closed)
    if frames:
        all_records = pd.concat(frames, ignore_index=True)
        all_records['signal_date'] = pd.to_datetime(all_records['signal_date'], errors='coerce')
        all_records = all_records.dropna(subset=['signal_date'])
        all_records['date_str'] = all_records['signal_date'].dt.strftime('%m/%d')
        all_records['month_str'] = all_records['signal_date'].dt.strftime('%Y-%m')

    strategies = ['A', 'B', 'C', 'D', 'E']
    rs_col_name = 'result_status' if (not closed_pos.empty and 'result_status' in closed_pos.columns) else 'status'

    # ── 전체 집계 ──
    total_detected = len(all_records)
    total_closed = len(closed_pos) if not closed_pos.empty else 0
    total_win = len(closed_pos[closed_pos[rs_col_name] == 'WIN']) if total_closed > 0 else 0
    total_loss = len(closed_pos[closed_pos[rs_col_name] == 'LOSS']) if total_closed > 0 else 0
    total_expired = len(closed_pos[closed_pos[rs_col_name] == 'EXPIRED']) if total_closed > 0 else 0
    total_open_cnt = len(open_pos[open_pos['status'].isin(['OPEN', 'PENDING'])]) if not open_pos.empty and 'status' in open_pos.columns else 0
    overall_wr = (total_win / total_closed * 100) if total_closed > 0 else 0

    # ══════════════════════════════════════════════════════════════════
    # 상단 요약 카드
    # ══════════════════════════════════════════════════════════════════
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(render_summary_card(f'{total_detected}', '총 탐지', '#64b5f6'), unsafe_allow_html=True)
    with c2:
        st.markdown(render_summary_card(f'{total_open_cnt}', '진행중', '#ffb74d'), unsafe_allow_html=True)
    with c3:
        st.markdown(render_summary_card(f'{total_win}', '익절 성공', '#81c784'), unsafe_allow_html=True)
    with c4:
        st.markdown(render_summary_card(f'{total_loss + total_expired}', '손절/기간초과', '#e57373'), unsafe_allow_html=True)
    with c5:
        wr_color = '#81c784' if overall_wr >= 80 else '#ffb74d' if overall_wr >= 50 else '#e57373'
        st.markdown(render_summary_card(f'{overall_wr:.1f}%', '실전 승률', wr_color), unsafe_allow_html=True)

    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

    # ── 서브 탭 ──
    h_daily, h_monthly, h_winrate, h_active, h_closed_detail, h_ticker = st.tabs([
        "📅 일별 성적", "📆 월별 성적", "🏆 전략별 승률", "🟡 진행중 포지션", "📋 청산 내역", "🔍 티커별"
    ])

    # ══════════════════════════════════════════════════════════════════
    # 1) 일별 매트릭스
    # ══════════════════════════════════════════════════════════════════
    with h_daily:
        if all_records.empty:
            st.markdown('<div class="no-signal">아직 데이터가 없습니다. 스캐너 실행 후 데이터가 쌓이면 여기에 일별 성적이 표시됩니다.</div>', unsafe_allow_html=True)
        else:
            cutoff = all_records['signal_date'].max() - pd.Timedelta(days=30)
            recent = all_records[all_records['signal_date'] >= cutoff].copy()
            date_order = sorted(recent['date_str'].unique(),
                                key=lambda x: pd.to_datetime(x, format='%m/%d'))

            det_g = recent.groupby(['strategy', 'date_str']).size().unstack(fill_value=0)
            wins_r = recent[recent['result'] == 'WIN']
            ach_g = wins_r.groupby(['strategy', 'date_str']).size().unstack(fill_value=0) if not wins_r.empty else pd.DataFrame(0, index=strategies, columns=date_order)

            for s in strategies:
                if s not in det_g.index: det_g.loc[s] = 0
                if s not in ach_g.index: ach_g.loc[s] = 0
            for d in date_order:
                if d not in det_g.columns: det_g[d] = 0
                if d not in ach_g.columns: ach_g[d] = 0

            # HTML 테이블 생성
            html = '<table class="matrix-table"><thead><tr><th class="strat-col">전략</th>'
            for d in date_order:
                html += f'<th>{d}</th>'
            html += '</tr></thead><tbody>'

            for s in strategies:
                html += f'<tr><td class="strat-label">{strat_badge(s)} {STRAT_NAMES.get(s,"")}</td>'
                for d in date_order:
                    det_v = int(det_g.loc[s, d]) if d in det_g.columns else 0
                    ach_v = int(ach_g.loc[s, d]) if d in ach_g.columns else 0
                    html += f'<td>{cell_html(ach_v, det_v)}</td>'
                html += '</tr>'

            html += '</tbody></table>'
            st.markdown(f'<p style="color:#888; font-size:0.85em; margin-bottom:8px;">최근 30일 — 셀: <b>익절 성공건수 / 총 탐지건수</b> (초록=전부달성, 주황=일부달성, 회색=미탐지)</p>{html}', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════
    # 2) 월별 매트릭스
    # ══════════════════════════════════════════════════════════════════
    with h_monthly:
        if all_records.empty:
            st.markdown('<div class="no-signal">아직 데이터가 없습니다.</div>', unsafe_allow_html=True)
        else:
            month_order = sorted(all_records['month_str'].unique())
            m_det_g = all_records.groupby(['strategy', 'month_str']).size().unstack(fill_value=0)
            m_wins = all_records[all_records['result'] == 'WIN']
            m_ach_g = m_wins.groupby(['strategy', 'month_str']).size().unstack(fill_value=0) if not m_wins.empty else pd.DataFrame(0, index=strategies, columns=month_order)

            for s in strategies:
                if s not in m_det_g.index: m_det_g.loc[s] = 0
                if s not in m_ach_g.index: m_ach_g.loc[s] = 0
            for m in month_order:
                if m not in m_det_g.columns: m_det_g[m] = 0
                if m not in m_ach_g.columns: m_ach_g[m] = 0

            # 달성/탐지 합산 테이블
            html = '<table class="matrix-table"><thead><tr><th class="strat-col">전략</th>'
            for m in month_order:
                html += f'<th>{m}</th>'
            html += '</tr></thead><tbody>'
            for s in strategies:
                html += f'<tr><td class="strat-label">{strat_badge(s)} {STRAT_NAMES.get(s,"")}</td>'
                for m in month_order:
                    det_v = int(m_det_g.loc[s, m]) if m in m_det_g.columns else 0
                    ach_v = int(m_ach_g.loc[s, m]) if m in m_ach_g.columns else 0
                    html += f'<td>{cell_html(ach_v, det_v)}</td>'
                html += '</tr>'
            html += '</tbody></table>'
            st.markdown(f'<p style="color:#888; font-size:0.85em; margin-bottom:8px;">월별 종합 — 셀: <b>익절 성공건수 / 총 탐지건수</b></p>{html}', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════
    # 3) 전략별 승률
    # ══════════════════════════════════════════════════════════════════
    with h_winrate:
        html = '<table class="matrix-table"><thead><tr>'
        html += '<th class="strat-col">전략</th><th>익절 목표</th><th>총 탐지건수</th><th>청산 완료</th>'
        html += '<th>익절 성공</th><th>손절 처리</th><th>보유기간 초과청산</th><th>현재 진행중</th>'
        html += '<th>실전 승률</th><th>백테스트 승률</th><th>건당 평균수익률</th>'
        html += '</tr></thead><tbody>'

        for s in strategies:
            s_all = all_records[all_records['strategy'] == s] if not all_records.empty else pd.DataFrame()
            s_closed = closed_pos[closed_pos['strategy'] == s] if not closed_pos.empty and 'strategy' in closed_pos.columns else pd.DataFrame()
            s_open_df = open_pos[(open_pos['strategy'] == s) & (open_pos['status'].isin(['OPEN', 'PENDING']))] if not open_pos.empty and 'strategy' in open_pos.columns and 'status' in open_pos.columns else pd.DataFrame()

            n_det = len(s_all)
            n_closed = len(s_closed)
            n_win = len(s_closed[s_closed[rs_col_name] == 'WIN']) if n_closed > 0 else 0
            n_loss = len(s_closed[s_closed[rs_col_name] == 'LOSS']) if n_closed > 0 else 0
            n_exp = len(s_closed[s_closed[rs_col_name] == 'EXPIRED']) if n_closed > 0 else 0
            n_active = len(s_open_df)
            wr = (n_win / n_closed * 100) if n_closed > 0 else 0

            avg_ret = 0
            if n_closed > 0 and 'result_pct' in s_closed.columns:
                avg_ret = safe_float(pd.to_numeric(s_closed['result_pct'], errors='coerce').mean())

            wr_class = 'cell-win' if wr >= 80 else 'cell-partial' if wr > 0 else 'cell-none'
            ret_class = 'cell-win' if avg_ret > 0 else 'cell-loss' if avg_ret < 0 else 'cell-none'
            bt_wr = STRAT_BT_WR.get(s, '—')

            html += f'<tr>'
            html += f'<td class="strat-label">{strat_badge(s)} {STRAT_NAMES.get(s,"")}</td>'
            html += f'<td style="font-weight:700">{STRAT_TP.get(s,"")}</td>'
            html += f'<td>{n_det}</td>'
            html += f'<td>{n_closed if n_closed > 0 else "—"}</td>'
            html += f'<td class="cell-win">{n_win if n_win > 0 else "—"}</td>'
            html += f'<td class="{"status-loss" if n_loss > 0 else "cell-none"}">{n_loss if n_loss > 0 else "—"}</td>'
            html += f'<td class="{"status-expired" if n_exp > 0 else "cell-none"}">{n_exp if n_exp > 0 else "—"}</td>'
            html += f'<td class="{"status-open" if n_active > 0 else "cell-none"}">{n_active if n_active > 0 else "—"}</td>'
            html += f'<td class="{wr_class}" style="font-size:1.05em">{wr:.1f}%</td>' if n_closed > 0 else '<td class="cell-none">—</td>'
            html += f'<td style="color:#888">{bt_wr}</td>'
            html += f'<td class="{ret_class}">{avg_ret:+.1f}%</td>' if n_closed > 0 else '<td class="cell-none">—</td>'
            html += f'</tr>'

        html += '</tbody></table>'
        st.markdown(html, unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════
    # 4) 진행중 포지션
    # ══════════════════════════════════════════════════════════════════
    with h_active:
        active = pd.DataFrame()
        if not open_pos.empty and 'status' in open_pos.columns:
            active = open_pos[open_pos['status'].isin(['PENDING', 'OPEN'])]
        if not active.empty:
            html = '<table class="pos-table"><thead><tr>'
            html += '<th>전략</th><th>종목(티커)</th><th>신호 발생일</th><th>매수일(D+1)</th>'
            html += '<th>매수가</th><th>현재가</th><th>현재 손익률</th>'
            html += '<th>목표 익절가</th><th>보유중 최고가</th><th>최고가 도달일</th>'
            html += '<th>익절목표 달성률</th><th>익절까지 남은%</th>'
            html += '<th>보유일/최대</th><th>현재 상태</th>'
            html += '</tr></thead><tbody>'

            for _, row in active.iterrows():
                s = safe_str(row.get('strategy'))
                tk = safe_str(row.get('ticker'))
                sig_dt = safe_str(row.get('signal_date'))
                ent_dt = safe_str(row.get('entry_date'))
                ent_pr_raw = safe_str(row.get('entry_price'))
                cur_pr_raw = safe_str(row.get('current_price'))
                tp_pr_raw = safe_str(row.get('tp_price'))
                max_pr_raw = safe_str(row.get('max_price'))
                max_pr_dt = safe_str(row.get('max_price_date'))
                ach_pct_raw = safe_str(row.get('achievement_pct'))
                chg_pct_raw = safe_str(row.get('change_pct'))
                days_held = safe_str(row.get('days_held'))
                mh = safe_str(row.get('max_hold'))
                status = safe_str(row.get('status'))

                ent_dt_html = f'<span class="price-none">{ent_dt}</span>' if ent_dt == '—' else ent_dt
                ent_pr_html = f'<span class="price-none">—</span>' if ent_pr_raw == '—' else f'${ent_pr_raw}'
                cur_pr_html = f'<span class="price-none">—</span>' if cur_pr_raw == '—' else f'${cur_pr_raw}'
                tp_pr_html = f'${tp_pr_raw}' if tp_pr_raw != '—' else '—'
                max_pr_html = f'${max_pr_raw}' if max_pr_raw != '—' else '—'
                status_class = 'status-open' if status == 'OPEN' else 'status-pending'

                # 수익률 색상
                chg_pct_val = safe_float(chg_pct_raw)
                chg_class = 'cell-win' if chg_pct_val > 0 else 'status-loss' if chg_pct_val < 0 else 'cell-none'
                chg_html = f'<span class="{chg_class}">{chg_pct_val:+.1f}%</span>' if chg_pct_raw != '—' else '—'

                # TP 달성률 프로그레스
                ach_val = safe_float(ach_pct_raw)
                ach_color = '#00c853' if ach_val >= 100 else '#ff9100' if ach_val >= 50 else '#e57373'
                if ach_pct_raw != '—' and ach_val > 0:
                    ach_html = f'''<div style="display:flex;align-items:center;gap:4px">
                        <div style="flex:1;background:#eee;border-radius:4px;height:8px;min-width:40px">
                            <div style="width:{min(ach_val,100):.0f}%;background:{ach_color};height:100%;border-radius:4px"></div>
                        </div>
                        <span style="font-size:0.82em;font-weight:700;color:{ach_color}">{ach_val:.0f}%</span>
                    </div>'''
                else:
                    ach_html = '<span class="price-none">—</span>'

                # TP 잔여거리 계산
                tp_remain_html = '—'
                cur_f = safe_float(cur_pr_raw)
                tp_f = safe_float(tp_pr_raw)
                if cur_f > 0 and tp_f > 0:
                    remain_pct = (tp_f - cur_f) / cur_f * 100
                    if remain_pct > 0:
                        tp_remain_html = f'<span style="color:#2979ff;font-weight:600">+{remain_pct:.1f}%</span>'
                    else:
                        tp_remain_html = f'<span class="cell-win">달성</span>'

                # 보유일 / 최대보유
                days_html = f'{days_held}' if days_held != '—' else '—'
                if days_held != '—' and mh != '—':
                    days_html = f'{days_held}/{mh}일'

                html += f'<tr>'
                html += f'<td>{strat_badge(s)}</td>'
                html += f'<td style="font-weight:700">{tk}</td>'
                html += f'<td>{sig_dt}</td>'
                html += f'<td>{ent_dt_html}</td>'
                html += f'<td>{ent_pr_html}</td>'
                html += f'<td>{cur_pr_html}</td>'
                html += f'<td>{chg_html}</td>'
                html += f'<td>{tp_pr_html}</td>'
                html += f'<td style="font-weight:600">{max_pr_html}</td>'
                html += f'<td>{max_pr_dt}</td>'
                html += f'<td>{ach_html}</td>'
                html += f'<td>{tp_remain_html}</td>'
                html += f'<td>{days_html}</td>'
                html += f'<td class="{status_class}">{status}</td>'
                html += f'</tr>'

            html += '</tbody></table>'
            st.markdown(html, unsafe_allow_html=True)
            st.markdown('<p style="color:#888;font-size:0.8em;margin-top:8px">TP달성률 = (최고가-진입가) / (익절가-진입가) × 100 | TP잔여 = 현재가에서 익절가까지 남은 %</p>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="no-signal">진행 중인 포지션이 없습니다</div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════
    # 5) 청산 내역 상세
    # ══════════════════════════════════════════════════════════════════
    with h_closed_detail:
        if not closed_pos.empty and 'strategy' in closed_pos.columns:
            # 최신순 정렬
            cp_sorted = closed_pos.copy()
            cp_sorted['_close_dt'] = pd.to_datetime(cp_sorted.get('close_date', ''), errors='coerce')
            cp_sorted = cp_sorted.sort_values('_close_dt', ascending=False)

            html = '<table class="pos-table"><thead><tr>'
            html += '<th>전략</th><th>종목(티커)</th><th>신호 발생일</th><th>매수일(D+1)</th><th>매수가</th>'
            html += '<th>청산 결과</th><th>청산일</th><th>청산가</th><th>최종 손익률</th>'
            html += '<th>익절가 도달일</th><th>보유중 최고가</th><th>최고가 기록일</th><th>익절목표 접근률</th>'
            html += '</tr></thead><tbody>'

            for _, row in cp_sorted.iterrows():
                s = safe_str(row.get('strategy'))
                tk = safe_str(row.get('ticker'))
                sig_dt = safe_str(row.get('signal_date'))
                ent_dt = safe_str(row.get('entry_date'))
                ent_pr = safe_str(row.get('entry_price'))
                result = safe_str(row.get(rs_col_name, 'status'))
                close_dt = safe_str(row.get('close_date'))
                close_pr = safe_str(row.get('close_price'))
                result_pct_raw = safe_str(row.get('result_pct'))
                tp_hit_dt = safe_str(row.get('tp_hit_date'))
                max_pr = safe_str(row.get('max_price'))
                max_pr_dt = safe_str(row.get('max_price_date'))
                max_ach_raw = safe_str(row.get('max_achievement_pct'))

                # 결과 색상
                if result == 'WIN':
                    result_html = '<span class="status-win">WIN</span>'
                elif result == 'LOSS':
                    result_html = '<span class="status-loss">LOSS</span>'
                elif result == 'EXPIRED':
                    result_html = '<span class="status-expired">EXPIRED</span>'
                else:
                    result_html = result

                # 수익률 색상
                rp_val = safe_float(result_pct_raw)
                rp_class = 'cell-win' if rp_val > 0 else 'status-loss' if rp_val < 0 else 'cell-none'
                rp_html = f'<span class="{rp_class}">{rp_val:+.1f}%</span>' if result_pct_raw != '—' else '—'

                # TP 달성일: 달성했으면 날짜, 아니면 "미달성"
                if tp_hit_dt != '—':
                    tp_hit_html = f'<span class="cell-win">{tp_hit_dt}</span>'
                else:
                    tp_hit_html = '<span class="price-none">미달성</span>'

                # 최고가/최고가일 — 미달성 시 "여기까지 도달" 느낌
                max_pr_html = f'${max_pr}' if max_pr != '—' else '—'
                if tp_hit_dt == '—' and max_pr != '—':
                    # TP 미달성인데 최고가가 있으면 강조
                    max_pr_html = f'<span style="color:#ff9100;font-weight:700">${max_pr}</span>'
                    max_pr_dt_html = f'<span style="color:#ff9100">{max_pr_dt}</span>'
                else:
                    max_pr_dt_html = max_pr_dt

                # 최대 달성률
                max_ach_val = safe_float(max_ach_raw)
                if max_ach_raw != '—' and max_ach_val > 0:
                    ach_color = '#00c853' if max_ach_val >= 100 else '#ff9100' if max_ach_val >= 50 else '#e57373'
                    max_ach_html = f'<span style="color:{ach_color};font-weight:700">{max_ach_val:.0f}%</span>'
                else:
                    max_ach_html = '—'

                ent_pr_html = f'${ent_pr}' if ent_pr != '—' else '—'
                close_pr_html = f'${close_pr}' if close_pr != '—' else '—'

                html += f'<tr>'
                html += f'<td>{strat_badge(s)}</td>'
                html += f'<td style="font-weight:700">{tk}</td>'
                html += f'<td>{sig_dt}</td>'
                html += f'<td>{ent_dt}</td>'
                html += f'<td>{ent_pr_html}</td>'
                html += f'<td>{result_html}</td>'
                html += f'<td>{close_dt}</td>'
                html += f'<td>{close_pr_html}</td>'
                html += f'<td>{rp_html}</td>'
                html += f'<td>{tp_hit_html}</td>'
                html += f'<td>{max_pr_html}</td>'
                html += f'<td>{max_pr_dt_html}</td>'
                html += f'<td>{max_ach_html}</td>'
                html += f'</tr>'

            html += '</tbody></table>'
            st.markdown(html, unsafe_allow_html=True)
            st.markdown('''<p style="color:#888;font-size:0.8em;margin-top:8px">
                TP달성일 = 일중 고가가 익절가에 도달한 최초 날짜 |
                미달성 시 <span style="color:#ff9100">주황색</span>으로 최고가·날짜를 표시 (가장 가까이 접근한 시점) |
                최대달성률 = 보유기간 중 최고가 기준 TP 목표 대비 달성 비율
            </p>''', unsafe_allow_html=True)
        else:
            st.markdown('<div class="no-signal">청산된 포지션이 없습니다</div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════
    # 6) 티커별 — 그룹 헤더 + 개별 포지션 상세
    # ══════════════════════════════════════════════════════════════════
    with h_ticker:
        # ── 모든 포지션을 통합 DataFrame으로 합치기 ──
        tk_rows = []

        if not closed_pos.empty and 'ticker' in closed_pos.columns:
            for _, row in closed_pos.iterrows():
                result = safe_str(row.get(rs_col_name, 'status'))
                tk_rows.append({
                    'ticker': safe_str(row.get('ticker')),
                    'strategy': safe_str(row.get('strategy')),
                    'signal_date': safe_str(row.get('signal_date')),
                    'entry_date': safe_str(row.get('entry_date')),
                    'entry_price': safe_str(row.get('entry_price')),
                    'tp_price': safe_str(row.get('tp_price')),
                    'current_price': safe_str(row.get('close_price')),
                    'max_price': safe_str(row.get('max_price')),
                    'max_price_date': safe_str(row.get('max_price_date')),
                    'result': result,
                    'result_pct': safe_str(row.get('result_pct')),
                    'tp_hit_date': safe_str(row.get('tp_hit_date')),
                    'max_ach': safe_str(row.get('max_achievement_pct')),
                    'close_date': safe_str(row.get('close_date')),
                    'is_open': False,
                    'change_pct': safe_str(row.get('result_pct')),
                    'achievement_pct': safe_str(row.get('max_achievement_pct')),
                    'days_held': safe_str(row.get('days_held')),
                })

        if not open_pos.empty and 'ticker' in open_pos.columns:
            for _, row in open_pos.iterrows():
                status = safe_str(row.get('status'))
                tk_rows.append({
                    'ticker': safe_str(row.get('ticker')),
                    'strategy': safe_str(row.get('strategy')),
                    'signal_date': safe_str(row.get('signal_date')),
                    'entry_date': safe_str(row.get('entry_date')),
                    'entry_price': safe_str(row.get('entry_price')),
                    'tp_price': safe_str(row.get('tp_price')),
                    'current_price': safe_str(row.get('current_price')),
                    'max_price': safe_str(row.get('max_price')),
                    'max_price_date': safe_str(row.get('max_price_date')),
                    'result': status,
                    'result_pct': safe_str(row.get('change_pct')),
                    'tp_hit_date': '—',
                    'max_ach': safe_str(row.get('achievement_pct')),
                    'close_date': '—',
                    'is_open': True,
                    'change_pct': safe_str(row.get('change_pct')),
                    'achievement_pct': safe_str(row.get('achievement_pct')),
                    'days_held': safe_str(row.get('days_held')),
                })

        if not tk_rows:
            st.markdown('<div class="no-signal">아직 추적 중인 티커가 없습니다</div>', unsafe_allow_html=True)
        else:
            # 티커별 그룹핑
            from collections import defaultdict
            ticker_groups = defaultdict(list)
            for r in tk_rows:
                if r['ticker'] != '—':
                    ticker_groups[r['ticker']].append(r)

            # 각 티커 내에서 signal_date 내림차순, 티커 간에는 최신 신호일 순
            for tk in ticker_groups:
                ticker_groups[tk].sort(key=lambda x: x['signal_date'], reverse=True)
            sorted_tickers = sorted(ticker_groups.items(),
                                    key=lambda x: x[1][0]['signal_date'] if x[1] else '', reverse=True)

            st.markdown(f'<p style="color:#888;font-size:0.85em;margin-bottom:8px">전체 {len(sorted_tickers)}개 종목 — 티커별 모든 포지션 상세</p>', unsafe_allow_html=True)

            html = ''
            for tk, positions in sorted_tickers:
                # ── 티커 그룹 집계 ──
                n_total = len(positions)
                n_win = sum(1 for p in positions if p['result'] == 'WIN')
                n_loss = sum(1 for p in positions if p['result'] == 'LOSS')
                n_exp = sum(1 for p in positions if p['result'] == 'EXPIRED')
                n_active = sum(1 for p in positions if p['result'] in ('OPEN', 'PENDING'))
                n_closed = n_win + n_loss + n_exp
                wr = (n_win / n_closed * 100) if n_closed > 0 else 0
                strats = sorted(set(p['strategy'] for p in positions if p['strategy'] != '—'))
                badges = ' '.join(strat_badge(s_) for s_ in strats)

                wr_color = '#00c853' if wr >= 80 else '#ff9100' if wr > 0 else '#888'
                wr_txt = f'{wr:.0f}%' if n_closed > 0 else '—'

                # 요약 바
                summary_parts = []
                if n_win > 0:
                    summary_parts.append(f'<span class="status-win">{n_win}W</span>')
                if n_loss > 0:
                    summary_parts.append(f'<span class="status-loss">{n_loss}L</span>')
                if n_exp > 0:
                    summary_parts.append(f'<span class="status-expired">{n_exp}E</span>')
                if n_active > 0:
                    summary_parts.append(f'<span class="status-open">{n_active} 진행</span>')
                summary_str = ' / '.join(summary_parts) if summary_parts else '—'

                # ── 티커 헤더 (진한 배경, 큰 글씨) ──
                html += f'''<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:10px;
                    padding:14px 20px;margin:16px 0 2px 0;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
                    <div style="display:flex;align-items:center;gap:12px">
                        <span style="color:#fff;font-weight:800;font-size:1.3em">{tk}</span>
                        <span>{badges}</span>
                    </div>
                    <div style="display:flex;align-items:center;gap:16px;color:#a0a0c0;font-size:0.88em">
                        <span>{n_total}건</span>
                        <span>{summary_str}</span>
                        <span style="color:{wr_color};font-weight:700;font-size:1.1em">승률 {wr_txt}</span>
                    </div>
                </div>'''

                # ── 해당 티커의 개별 포지션 테이블 ──
                html += '<table class="pos-table" style="margin-bottom:0"><thead><tr>'
                html += '<th>전략</th><th>신호 발생일</th><th>매수일(D+1)</th><th>매수가</th>'
                html += '<th>청산 결과</th><th>최종 손익률</th>'
                html += '<th>익절가 도달일</th><th>보유중 최고가</th><th>최고가 기록일</th><th>익절목표 접근률</th>'
                html += '</tr></thead><tbody>'

                for p in positions:
                    s_ = p['strategy']
                    sig_dt = p['signal_date']
                    ent_dt = p['entry_date']
                    ent_pr = p['entry_price']
                    result = p['result']
                    rp_raw = p['result_pct']
                    tp_hit = p['tp_hit_date']
                    max_pr = p['max_price']
                    max_pr_dt = p['max_price_date']
                    max_ach_raw = p['max_ach']

                    ent_pr_html = f'${ent_pr}' if ent_pr != '—' else '<span class="price-none">—</span>'

                    # 결과 뱃지
                    if result == 'WIN':
                        res_html = '<span class="status-win">WIN</span>'
                    elif result == 'LOSS':
                        res_html = '<span class="status-loss">LOSS</span>'
                    elif result == 'EXPIRED':
                        res_html = '<span class="status-expired">EXPIRED</span>'
                    elif result == 'OPEN':
                        res_html = '<span class="status-open">OPEN</span>'
                    elif result == 'PENDING':
                        res_html = '<span class="status-pending">PENDING</span>'
                    else:
                        res_html = result

                    # 수익률
                    rp_val = safe_float(rp_raw)
                    rp_cls = 'cell-win' if rp_val > 0 else 'status-loss' if rp_val < 0 else 'cell-none'
                    rp_html = f'<span class="{rp_cls}">{rp_val:+.1f}%</span>' if rp_raw != '—' else '<span class="price-none">—</span>'

                    # TP 달성일
                    if tp_hit != '—':
                        tp_html = f'<span class="cell-win">{tp_hit}</span>'
                    else:
                        tp_html = '<span class="price-none">미달성</span>'

                    # 최고가 — TP 미달성 시 주황 강조
                    if max_pr != '—':
                        if tp_hit == '—':
                            mp_html = f'<span style="color:#ff9100;font-weight:700">${max_pr}</span>'
                            mpd_html = f'<span style="color:#ff9100">{max_pr_dt}</span>'
                        else:
                            mp_html = f'${max_pr}'
                            mpd_html = max_pr_dt
                    else:
                        mp_html = '<span class="price-none">—</span>'
                        mpd_html = '<span class="price-none">—</span>'

                    # TP 달성률 — 프로그레스바
                    ach_val = safe_float(max_ach_raw)
                    if max_ach_raw != '—' and ach_val > 0:
                        ac = '#00c853' if ach_val >= 100 else '#ff9100' if ach_val >= 50 else '#e57373'
                        ach_html = f'''<div style="display:flex;align-items:center;gap:4px">
                            <div style="flex:1;background:#eee;border-radius:4px;height:8px;min-width:40px">
                                <div style="width:{min(ach_val,100):.0f}%;background:{ac};height:100%;border-radius:4px"></div>
                            </div>
                            <span style="font-size:0.82em;font-weight:700;color:{ac}">{ach_val:.0f}%</span>
                        </div>'''
                    else:
                        ach_html = '<span class="price-none">—</span>'

                    html += f'<tr>'
                    html += f'<td>{strat_badge(s_)}</td>'
                    html += f'<td>{sig_dt}</td>'
                    html += f'<td>{ent_dt if ent_dt != "—" else "<span class=price-none>—</span>"}</td>'
                    html += f'<td>{ent_pr_html}</td>'
                    html += f'<td>{res_html}</td>'
                    html += f'<td>{rp_html}</td>'
                    html += f'<td>{tp_html}</td>'
                    html += f'<td>{mp_html}</td>'
                    html += f'<td>{mpd_html}</td>'
                    html += f'<td>{ach_html}</td>'
                    html += f'</tr>'

                html += '</tbody></table>'

            st.markdown(html, unsafe_allow_html=True)
            st.markdown('''<p style="color:#888;font-size:0.8em;margin-top:12px">
                각 종목의 전체 포지션 이력을 시간순으로 표시 |
                TP달성일 = 일중 고가가 익절가 도달한 최초 날짜 |
                <span style="color:#ff9100">주황색 최고가</span> = TP 미달성 시 가장 가까이 접근한 가격과 날짜 |
                TP달성률 바 = 목표 대비 최대 도달 비율
            </p>''', unsafe_allow_html=True)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("정보")
    st.markdown("""
    **US Stock Surge Scanner**

    매일 미국 장 마감 후 자동 스캔하여
    급락 후 반등 가능성이 높은 종목을 포착합니다.

    | 전략 | 목표 | 승률 | 보유 |
    |------|------|------|------|
    | A | +5% | 90.1% | 5일 |
    | B | +15% | 90.3% | 10일 |
    | C | +5% | 86.9% | 5일 |
    | D | +20% | 97.7% | 30일 |
    | E | +10% | 91.0% | 30일 |

    ---
    GitHub Actions로 평일 자동 실행
    """)
    st.divider()
    if tracker_info:
        st.markdown(f"**마지막 추적:** {tracker_info.get('last_tracked', 'N/A')}")
        st.markdown(f"진행중: {tracker_info.get('open_count', 0)}건 | "
                   f"대기: {tracker_info.get('pending_count', 0)}건 | "
                   f"청산: {tracker_info.get('closed_count', 0)}건")
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()
