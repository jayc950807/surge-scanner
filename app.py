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
# ─── Tab: History ─────────────────────────────────────────────────────────────
with tab_history:
    st.subheader("📊 실전 성적표")

    open_pos = load_open_positions()
    closed_pos = load_closed_positions()

    # ── 전체 데이터 합치기 (탐지건: open+closed, 달성건: closed의 WIN) ──
    # tracker.py는 closed_positions에 result_status(WIN/LOSS/EXPIRED) 사용
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

    # ── 서브 탭: 일별 | 월별 | 승률 | 진행중 ──
    h_daily, h_monthly, h_winrate, h_active = st.tabs([
        "📅 일별", "📆 월별", "🏆 승률", "🟡 진행중"
    ])

    # ════════════════════════════════════════════════════════════════════
    # 1) 일별: 날짜(가로) × 전략(세로) — 달성건/탐지건
    # ════════════════════════════════════════════════════════════════════
    with h_daily:
        if all_records.empty:
            st.markdown('<div class="no-signal">아직 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            # 최근 30일만 표시
            cutoff = all_records['signal_date'].max() - pd.Timedelta(days=30)
            recent = all_records[all_records['signal_date'] >= cutoff].copy()

            # 날짜 정렬
            date_order = sorted(recent['date_str'].unique(),
                                key=lambda x: pd.to_datetime(x, format='%m/%d'))

            # 탐지건 피벗
            detected = recent.groupby(['strategy', 'date_str']).size().reset_index(name='cnt')
            det_pivot = detected.pivot(index='strategy', columns='date_str', values='cnt').fillna(0).astype(int)

            # 달성건 피벗 (WIN만)
            wins_only = recent[recent['result'] == 'WIN']
            if not wins_only.empty:
                achieved = wins_only.groupby(['strategy', 'date_str']).size().reset_index(name='cnt')
                ach_pivot = achieved.pivot(index='strategy', columns='date_str', values='cnt').fillna(0).astype(int)
            else:
                ach_pivot = pd.DataFrame(0, index=strategies, columns=date_order)

            # 모든 전략 행 보장
            for s in strategies:
                if s not in det_pivot.index:
                    det_pivot.loc[s] = 0
                if s not in ach_pivot.index:
                    ach_pivot.loc[s] = 0

            # 날짜 열 정렬 & 일치시키기
            for col in date_order:
                if col not in det_pivot.columns:
                    det_pivot[col] = 0
                if col not in ach_pivot.columns:
                    ach_pivot[col] = 0

            det_pivot = det_pivot[date_order].reindex(strategies)
            ach_pivot = ach_pivot[date_order].reindex(strategies)

            # 달성/탐지 합쳐서 문자열로
            display_df = pd.DataFrame(index=strategies, columns=date_order)
            for s in strategies:
                for d in date_order:
                    det_val = int(det_pivot.loc[s, d])
                    ach_val = int(ach_pivot.loc[s, d])
                    if det_val == 0:
                        display_df.loc[s, d] = "—"
                    else:
                        display_df.loc[s, d] = f"{ach_val}/{det_val}"

            display_df.index.name = "전략"
            st.markdown("**최근 30일** — 각 셀: `달성건/탐지건`")
            st.dataframe(display_df, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # 2) 월별: 월(가로) × 전략(세로) — 탐지건 & 달성건
    # ════════════════════════════════════════════════════════════════════
    with h_monthly:
        if all_records.empty:
            st.markdown('<div class="no-signal">아직 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            month_order = sorted(all_records['month_str'].unique())

            # 탐지건
            m_det = all_records.groupby(['strategy', 'month_str']).size().reset_index(name='탐지')
            m_det_pivot = m_det.pivot(index='strategy', columns='month_str', values='탐지').fillna(0).astype(int)

            # 달성건
            m_wins = all_records[all_records['result'] == 'WIN']
            if not m_wins.empty:
                m_ach = m_wins.groupby(['strategy', 'month_str']).size().reset_index(name='달성')
                m_ach_pivot = m_ach.pivot(index='strategy', columns='month_str', values='달성').fillna(0).astype(int)
            else:
                m_ach_pivot = pd.DataFrame(0, index=strategies, columns=month_order)

            for s in strategies:
                if s not in m_det_pivot.index:
                    m_det_pivot.loc[s] = 0
                if s not in m_ach_pivot.index:
                    m_ach_pivot.loc[s] = 0

            for col in month_order:
                if col not in m_det_pivot.columns:
                    m_det_pivot[col] = 0
                if col not in m_ach_pivot.columns:
                    m_ach_pivot[col] = 0

            m_det_pivot = m_det_pivot[month_order].reindex(strategies)
            m_ach_pivot = m_ach_pivot[month_order].reindex(strategies)

            # 탐지건 테이블
            st.markdown("**📌 탐지건** (월별 신호 발생 수)")
            m_det_display = m_det_pivot.copy()
            m_det_display.index.name = "전략"
            st.dataframe(m_det_display, use_container_width=True)

            # 달성건 테이블
            st.markdown("**✅ 달성건** (월별 익절 성공 수)")
            m_ach_display = m_ach_pivot.copy()
            m_ach_display.index.name = "전략"
            st.dataframe(m_ach_display, use_container_width=True)

            # 달성/탐지 합산 테이블
            st.markdown("**📊 달성/탐지** (합산)")
            m_combined = pd.DataFrame(index=strategies, columns=month_order)
            for s in strategies:
                for m in month_order:
                    det_v = int(m_det_pivot.loc[s, m])
                    ach_v = int(m_ach_pivot.loc[s, m])
                    if det_v == 0:
                        m_combined.loc[s, m] = "—"
                    else:
                        m_combined.loc[s, m] = f"{ach_v}/{det_v}"
            m_combined.index.name = "전략"
            st.dataframe(m_combined, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # 3) 승률: 전략별 실제 승률 요약
    # ════════════════════════════════════════════════════════════════════
    with h_winrate:
        if closed_pos.empty:
            st.markdown('<div class="no-signal">아직 청산 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            winrate_rows = []
            for strat in strategies:
                s_all = all_records[all_records['strategy'] == strat] if not all_records.empty else pd.DataFrame()
                s_closed = closed_pos[closed_pos['strategy'] == strat] if not closed_pos.empty else pd.DataFrame()

                total_detected = len(s_all)
                total_closed = len(s_closed)
                rs_col = 'result_status' if 'result_status' in s_closed.columns else 'status'
                wins = len(s_closed[s_closed[rs_col] == 'WIN']) if not s_closed.empty else 0
                losses = len(s_closed[s_closed[rs_col] == 'LOSS']) if not s_closed.empty else 0
                expired = len(s_closed[s_closed[rs_col] == 'EXPIRED']) if not s_closed.empty else 0

                # 진행중 건수
                s_open = open_pos[
                    (open_pos['strategy'] == strat) &
                    (open_pos['status'].isin(['PENDING', 'OPEN']))
                ] if not open_pos.empty and 'status' in open_pos.columns else pd.DataFrame()
                in_progress = len(s_open)

                win_rate = (wins / total_closed * 100) if total_closed > 0 else 0

                # 평균 수익률
                if not s_closed.empty and 'result_pct' in s_closed.columns:
                    avg_ret = pd.to_numeric(s_closed['result_pct'], errors='coerce').mean()
                    avg_ret_str = f"{avg_ret:+.1f}%"
                else:
                    avg_ret_str = "—"

                winrate_rows.append({
                    '전략': strat,
                    '탐지건': total_detected,
                    '청산건': total_closed,
                    '✅ 익절': wins,
                    '❌ 손절': losses,
                    '⏰ 만기': expired,
                    '🔄 진행중': in_progress,
                    '승률': f"{win_rate:.1f}%",
                    '평균수익률': avg_ret_str,
                })

            wr_df = pd.DataFrame(winrate_rows)
            st.dataframe(wr_df, use_container_width=True, hide_index=True)

            # 전체 요약 메트릭
            st.divider()
            total_all = wr_df['청산건'].sum()
            total_wins = wr_df['✅ 익절'].sum()
            overall_wr = (total_wins / total_all * 100) if total_all > 0 else 0
            col1, col2, col3 = st.columns(3)
            col1.metric("총 청산건", f"{total_all}건")
            col2.metric("총 익절건", f"{total_wins}건")
            col3.metric("전체 승률", f"{overall_wr:.1f}%")

    # ════════════════════════════════════════════════════════════════════
    # 4) 진행중: 현재 PENDING/OPEN 포지션
    # ════════════════════════════════════════════════════════════════════
    with h_active:
        active = pd.DataFrame()
        if not open_pos.empty and 'status' in open_pos.columns:
            active = open_pos[open_pos['status'].isin(['PENDING', 'OPEN'])]
        if not active.empty:
            st.success(f"현재 진행 중: {len(active)}건")
            disp_cols = ['strategy', 'ticker', 'signal_date', 'entry_date',
                         'entry_price', 'tp_price', 'max_hold', 'status']
            avail = [c for c in disp_cols if c in active.columns]
            st.dataframe(
                active[avail].reset_index(drop=True),
                use_container_width=True,
                column_config={
                    "strategy": st.column_config.TextColumn("전략", width="small"),
                    "ticker": st.column_config.TextColumn("종목", width="small"),
                    "signal_date": st.column_config.TextColumn("신호일"),
                    "entry_date": st.column_config.TextColumn("진입일"),
                    "entry_price": st.column_config.TextColumn("진입가"),
                    "tp_price": st.column_config.TextColumn("익절가"),
                    "max_hold": st.column_config.TextColumn("최대보유"),
                    "status": st.column_config.TextColumn("상태"),
                },
            )
        else:
            st.markdown('<div class="no-signal">진행 중 포지션 없음</div>', unsafe_allow_html=True)
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
