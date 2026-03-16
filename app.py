#!/usr/bin/env python3
"""
================================================================================
  US Stock Surge Scanner — Streamlit Dashboard
  Strategy A/B/C/D/E 대시보드 + 상세 히스토리 추적
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

# ─── Custom CSS (모바일 반응형 + 히스토리 스타일) ─────────────────────────────
st.markdown("""
<style>
    .main .block-container { padding-top: 1rem; max-width: 1400px; }
    .stMetric > div { background: #f8f9fa; border-radius: 8px; padding: 12px; }
    .no-signal {
        text-align: center; padding: 40px; color: #888;
        font-size: 1.2em; background: #f8f9fa; border-radius: 12px;
    }
    /* 히스토리 카드 스타일 */
    .pos-card {
        background: #ffffff; border: 1px solid #e0e0e0;
        border-radius: 12px; padding: 16px; margin: 8px 0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .pos-card.win { border-left: 4px solid #00c853; }
    .pos-card.loss { border-left: 4px solid #ff1744; }
    .pos-card.expired { border-left: 4px solid #ff9100; }
    .pos-card.open { border-left: 4px solid #2979ff; }
    .pos-card.pending { border-left: 4px solid #9e9e9e; }
    .pos-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .pos-ticker { font-size: 1.3em; font-weight: 700; color: #1a1a1a; }
    .pos-strategy { font-size: 0.85em; padding: 2px 8px; border-radius: 12px; color: #fff; font-weight: 600; }
    .strat-A { background: #00c853; } .strat-B { background: #2979ff; }
    .strat-C { background: #ff9100; } .strat-D { background: #ff1744; }
    .strat-E { background: #7c4dff; }
    .pos-detail { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 8px; font-size: 0.88em; }
    .pos-detail-item { }
    .pos-detail-label { color: #888; font-size: 0.8em; }
    .pos-detail-value { font-weight: 600; color: #333; }
    .achievement-bar {
        height: 8px; border-radius: 4px; background: #e8e8e8; margin-top: 4px; overflow: hidden;
    }
    .achievement-fill {
        height: 100%; border-radius: 4px;
        transition: width 0.3s ease;
    }
    .badge-win { background: #e8f5e9; color: #2e7d32; padding: 2px 10px; border-radius: 12px; font-weight: 600; font-size: 0.85em; }
    .badge-loss { background: #ffebee; color: #c62828; padding: 2px 10px; border-radius: 12px; font-weight: 600; font-size: 0.85em; }
    .badge-expired { background: #fff3e0; color: #e65100; padding: 2px 10px; border-radius: 12px; font-weight: 600; font-size: 0.85em; }
    .badge-open { background: #e3f2fd; color: #1565c0; padding: 2px 10px; border-radius: 12px; font-weight: 600; font-size: 0.85em; }
    .badge-pending { background: #f5f5f5; color: #616161; padding: 2px 10px; border-radius: 12px; font-weight: 600; font-size: 0.85em; }
    @media (max-width: 768px) {
        .main .block-container { padding: 0.5rem; }
        .pos-detail { grid-template-columns: repeat(2, 1fr); }
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

@st.cache_data(ttl=300)
def load_tracker_summary():
    path = "data/tracker_summary.json"
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return None

# ─── Helper: Strategy config ─────────────────────────────────────────────────

STRATEGY_EMOJI = {'A': '🟢', 'B': '🔵', 'C': '🟠', 'D': '🔴', 'E': '🟣'}
STRATEGY_NAMES = {
    'A': '급락 반등 +5%',
    'B': '고수익 +15%',
    'C': '과매도 반등 +5%',
    'D': '초저가 폭락 +20%',
    'E': '급락 속반등 +10%',
}
STRATEGY_TP = {'A': 5, 'B': 15, 'C': 5, 'D': 20, 'E': 10}

# ─── Helper: Render position card ────────────────────────────────────────────

def safe_float(val, default=0):
    try:
        return float(val) if val and str(val).strip() not in ('', 'nan', 'None') else default
    except:
        return default

def render_position_card(row, status_type='open'):
    """포지션 카드를 HTML로 렌더링"""
    strategy = str(row.get('strategy', ''))
    ticker = str(row.get('ticker', ''))
    signal_date = str(row.get('signal_date', ''))
    signal_price = safe_float(row.get('signal_price'))
    entry_date = str(row.get('entry_date', ''))
    entry_price = safe_float(row.get('entry_price'))
    current_price = safe_float(row.get('current_price'))
    max_price = safe_float(row.get('max_price'))
    max_price_date = str(row.get('max_price_date', ''))
    min_price = safe_float(row.get('min_price'))
    achievement_pct = safe_float(row.get('achievement_pct'))
    days_held = str(row.get('days_held', '0'))
    tp_price = safe_float(row.get('tp_price'))
    sl_price = safe_float(row.get('sl_price'))
    change_pct = safe_float(row.get('change_pct'))
    last_updated = str(row.get('last_updated', ''))
    status = str(row.get('status', ''))

    # 청산 정보 (closed positions)
    close_date = str(row.get('close_date', ''))
    close_price = safe_float(row.get('close_price'))
    result_pct = safe_float(row.get('result_pct'))
    result_status = str(row.get('result_status', ''))
    tp_hit_date = str(row.get('tp_hit_date', ''))
    max_achievement_pct = safe_float(row.get('max_achievement_pct'))

    # 카드 클래스
    if result_status == 'WIN':
        card_class = 'win'
        badge = f'<span class="badge-win">✅ 익절 성공</span>'
    elif result_status == 'LOSS':
        card_class = 'loss'
        badge = f'<span class="badge-loss">❌ 손절</span>'
    elif result_status == 'EXPIRED':
        card_class = 'expired'
        badge = f'<span class="badge-expired">⏰ 만기 청산</span>'
    elif status == 'OPEN':
        card_class = 'open'
        badge = f'<span class="badge-open">🔄 진행중</span>'
    else:
        card_class = 'pending'
        badge = f'<span class="badge-pending">⏳ 대기</span>'

    strat_name = STRATEGY_NAMES.get(strategy, strategy)
    strat_emoji = STRATEGY_EMOJI.get(strategy, '')
    tp_target = STRATEGY_TP.get(strategy, 0)

    # 수익률 색상
    if status_type == 'closed':
        pct_val = result_pct
    else:
        pct_val = change_pct
    pct_color = '#00c853' if pct_val > 0 else '#ff1744' if pct_val < 0 else '#666'
    pct_str = f'{pct_val:+.2f}%' if pct_val != 0 else '—'

    # 달성률 바
    ach_val = max_achievement_pct if status_type == 'closed' else achievement_pct
    ach_val = min(ach_val, 100)
    ach_color = '#00c853' if ach_val >= 100 else '#ff9100' if ach_val >= 50 else '#2979ff'

    html = f'''
    <div class="pos-card {card_class}">
        <div class="pos-header">
            <div>
                <span class="pos-ticker">{ticker}</span>
                <span class="pos-strategy strat-{strategy}">{strat_emoji} {strategy} — {strat_name}</span>
            </div>
            <div>
                {badge}
            </div>
        </div>
        <div class="pos-detail">
            <div class="pos-detail-item">
                <div class="pos-detail-label">신호일</div>
                <div class="pos-detail-value">{signal_date}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">신호가</div>
                <div class="pos-detail-value">${signal_price:.2f}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">진입일</div>
                <div class="pos-detail-value">{entry_date if entry_date and entry_date != 'nan' else '—'}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">진입가 (D+1 시가)</div>
                <div class="pos-detail-value">${entry_price:.2f}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">익절 목표가</div>
                <div class="pos-detail-value">${tp_price:.2f} (+{tp_target}%)</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">손절가</div>
                <div class="pos-detail-value">{"${:.2f}".format(sl_price) if sl_price > 0 else "없음"}</div>
            </div>
    '''

    if status_type == 'closed':
        html += f'''
            <div class="pos-detail-item">
                <div class="pos-detail-label">청산일</div>
                <div class="pos-detail-value">{close_date}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">청산가</div>
                <div class="pos-detail-value">${close_price:.2f}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">최종 수익률</div>
                <div class="pos-detail-value" style="color:{pct_color}; font-size:1.1em">{pct_str}</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">보유일</div>
                <div class="pos-detail-value">{days_held}일</div>
            </div>
        '''
        if result_status == 'WIN' and tp_hit_date and tp_hit_date != 'nan':
            html += f'''
            <div class="pos-detail-item">
                <div class="pos-detail-label">익절 도달일</div>
                <div class="pos-detail-value" style="color:#00c853; font-weight:700">{tp_hit_date}</div>
            </div>
            '''
        if result_status in ('LOSS', 'EXPIRED'):
            html += f'''
            <div class="pos-detail-item">
                <div class="pos-detail-label">기간 중 최고가</div>
                <div class="pos-detail-value">${max_price:.2f} ({max_price_date if max_price_date and max_price_date != 'nan' else ''})</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">목표 달성률</div>
                <div class="pos-detail-value">{max_achievement_pct:.1f}%</div>
            </div>
            '''
    else:
        # OPEN / PENDING
        html += f'''
            <div class="pos-detail-item">
                <div class="pos-detail-label">현재가</div>
                <div class="pos-detail-value" style="color:{pct_color}; font-size:1.1em">${current_price:.2f} ({pct_str})</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">기간 중 최고가</div>
                <div class="pos-detail-value">${max_price:.2f} ({max_price_date if max_price_date and max_price_date != 'nan' else ''})</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">보유일</div>
                <div class="pos-detail-value">{days_held}일</div>
            </div>
            <div class="pos-detail-item">
                <div class="pos-detail-label">마지막 업데이트</div>
                <div class="pos-detail-value">{last_updated}</div>
            </div>
        '''

    # 달성률 바
    html += f'''
        </div>
        <div style="margin-top: 10px;">
            <div class="pos-detail-label">목표 달성률: {ach_val:.1f}%</div>
            <div class="achievement-bar">
                <div class="achievement-fill" style="width: {min(ach_val, 100)}%; background: {ach_color};"></div>
            </div>
        </div>
    </div>
    '''
    return html


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
        f"📍 추적: OPEN {tracker_info.get('open_count', 0)}건 | "
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


# ═══════════════════════════════════════════════════════════════════════════════
# 📊 히스토리 탭 — 완전 리디자인
# ═══════════════════════════════════════════════════════════════════════════════
with tab_history:
    st.subheader("📊 실전 추적 히스토리")

    open_pos = load_open_positions()
    closed_pos = load_closed_positions()

    # ── 상단 요약 메트릭 ──
    total_open = len(open_pos[open_pos['status'] == 'OPEN']) if not open_pos.empty and 'status' in open_pos.columns else 0
    total_pending = len(open_pos[open_pos['status'] == 'PENDING']) if not open_pos.empty and 'status' in open_pos.columns else 0
    total_closed = len(closed_pos) if not closed_pos.empty else 0

    total_win = len(closed_pos[closed_pos['result_status'] == 'WIN']) if not closed_pos.empty and 'result_status' in closed_pos.columns else 0
    total_loss = len(closed_pos[closed_pos['result_status'] == 'LOSS']) if not closed_pos.empty and 'result_status' in closed_pos.columns else 0
    total_expired = len(closed_pos[closed_pos['result_status'] == 'EXPIRED']) if not closed_pos.empty and 'result_status' in closed_pos.columns else 0
    overall_wr = (total_win / total_closed * 100) if total_closed > 0 else 0

    avg_return = 0
    if not closed_pos.empty and 'result_pct' in closed_pos.columns:
        avg_return = pd.to_numeric(closed_pos['result_pct'], errors='coerce').mean()
        avg_return = avg_return if not pd.isna(avg_return) else 0

    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric("🔄 진행중", f"{total_open}건")
    mc2.metric("⏳ 대기", f"{total_pending}건")
    mc3.metric("✅ 익절", f"{total_win}건")
    mc4.metric("❌ 손절", f"{total_loss}건")
    mc5.metric("🏆 승률", f"{overall_wr:.1f}%")
    mc6.metric("📈 평균수익률", f"{avg_return:+.1f}%")

    st.divider()

    # ── 서브 탭 ──
    h_active, h_all_history, h_by_strategy, h_stats = st.tabs([
        "🔄 진행중 포지션",
        "📋 전체 히스토리",
        "📊 전략별 상세",
        "🏆 성과 통계",
    ])

    # ════════════════════════════════════════════════════════════════════
    # 1) 진행중 포지션 — 카드 형태
    # ════════════════════════════════════════════════════════════════════
    with h_active:
        active_positions = pd.DataFrame()
        if not open_pos.empty and 'status' in open_pos.columns:
            active_positions = open_pos[open_pos['status'].isin(['OPEN', 'PENDING'])]

        if active_positions.empty:
            st.markdown('<div class="no-signal">진행 중인 포지션이 없습니다</div>', unsafe_allow_html=True)
        else:
            # OPEN 먼저, PENDING 나중
            open_first = active_positions[active_positions['status'] == 'OPEN']
            pending_second = active_positions[active_positions['status'] == 'PENDING']

            if not open_first.empty:
                st.markdown(f"### 🔄 진행중 ({len(open_first)}건)")
                for _, row in open_first.iterrows():
                    st.markdown(render_position_card(row, 'open'), unsafe_allow_html=True)

            if not pending_second.empty:
                st.markdown(f"### ⏳ 진입 대기 ({len(pending_second)}건)")
                for _, row in pending_second.iterrows():
                    st.markdown(render_position_card(row, 'open'), unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════
    # 2) 전체 히스토리 — 필터 + 테이블 + 카드
    # ════════════════════════════════════════════════════════════════════
    with h_all_history:
        if closed_pos.empty:
            st.markdown('<div class="no-signal">아직 청산된 포지션이 없습니다</div>', unsafe_allow_html=True)
        else:
            # 필터
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            with filter_col1:
                filter_strategy = st.multiselect(
                    "전략 선택",
                    options=['A', 'B', 'C', 'D', 'E'],
                    default=['A', 'B', 'C', 'D', 'E'],
                    key='hist_strat_filter'
                )
            with filter_col2:
                filter_status = st.multiselect(
                    "결과 선택",
                    options=['WIN', 'LOSS', 'EXPIRED'],
                    default=['WIN', 'LOSS', 'EXPIRED'],
                    key='hist_status_filter'
                )
            with filter_col3:
                view_mode = st.radio("보기 방식", ["카드", "테이블"], horizontal=True, key='hist_view')

            # 필터 적용
            filtered = closed_pos.copy()
            if 'strategy' in filtered.columns:
                filtered = filtered[filtered['strategy'].isin(filter_strategy)]
            if 'result_status' in filtered.columns:
                filtered = filtered[filtered['result_status'].isin(filter_status)]

            # 날짜순 정렬 (최신순)
            if 'signal_date' in filtered.columns:
                filtered = filtered.sort_values('signal_date', ascending=False)

            st.markdown(f"**총 {len(filtered)}건**")

            if view_mode == "카드":
                # 카드 뷰 (최대 50개)
                display_count = min(len(filtered), 50)
                for i, (_, row) in enumerate(filtered.head(display_count).iterrows()):
                    st.markdown(render_position_card(row, 'closed'), unsafe_allow_html=True)
                if len(filtered) > display_count:
                    st.info(f"최근 {display_count}건만 표시됩니다. 테이블 뷰로 전체를 확인하세요.")
            else:
                # 테이블 뷰
                table_cols = [
                    'strategy', 'ticker', 'signal_date', 'signal_price',
                    'entry_date', 'entry_price', 'tp_price', 'sl_price',
                    'close_date', 'close_price', 'result_pct', 'result_status',
                    'tp_hit_date', 'max_price', 'max_price_date',
                    'achievement_pct', 'max_achievement_pct', 'days_held',
                ]
                avail_cols = [c for c in table_cols if c in filtered.columns]
                st.dataframe(
                    filtered[avail_cols].reset_index(drop=True),
                    use_container_width=True,
                    column_config={
                        "strategy": st.column_config.TextColumn("전략", width="small"),
                        "ticker": st.column_config.TextColumn("종목", width="small"),
                        "signal_date": st.column_config.TextColumn("신호일"),
                        "signal_price": st.column_config.TextColumn("신호가"),
                        "entry_date": st.column_config.TextColumn("진입일"),
                        "entry_price": st.column_config.TextColumn("진입가"),
                        "tp_price": st.column_config.TextColumn("익절가"),
                        "sl_price": st.column_config.TextColumn("손절가"),
                        "close_date": st.column_config.TextColumn("청산일"),
                        "close_price": st.column_config.TextColumn("청산가"),
                        "result_pct": st.column_config.TextColumn("수익률%"),
                        "result_status": st.column_config.TextColumn("결과"),
                        "tp_hit_date": st.column_config.TextColumn("익절도달일"),
                        "max_price": st.column_config.TextColumn("최고가"),
                        "max_price_date": st.column_config.TextColumn("최고가일"),
                        "achievement_pct": st.column_config.TextColumn("달성률%"),
                        "max_achievement_pct": st.column_config.TextColumn("최대달성률%"),
                        "days_held": st.column_config.TextColumn("보유일"),
                    },
                    height=600,
                )

    # ════════════════════════════════════════════════════════════════════
    # 3) 전략별 상세 분석
    # ════════════════════════════════════════════════════════════════════
    with h_by_strategy:
        if closed_pos.empty and (open_pos.empty or 'strategy' not in open_pos.columns):
            st.markdown('<div class="no-signal">아직 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            strategies = ['A', 'B', 'C', 'D', 'E']
            selected_strat = st.selectbox("전략 선택", strategies,
                                         format_func=lambda x: f"{STRATEGY_EMOJI.get(x, '')} 전략 {x} — {STRATEGY_NAMES.get(x, '')}",
                                         key='strat_detail_select')

            st.markdown(f"### {STRATEGY_EMOJI.get(selected_strat, '')} 전략 {selected_strat} — {STRATEGY_NAMES.get(selected_strat, '')}")

            # 해당 전략의 포지션들
            strat_open = open_pos[open_pos['strategy'] == selected_strat] if not open_pos.empty and 'strategy' in open_pos.columns else pd.DataFrame()
            strat_closed = closed_pos[closed_pos['strategy'] == selected_strat] if not closed_pos.empty and 'strategy' in closed_pos.columns else pd.DataFrame()

            # 메트릭
            sc1, sc2, sc3, sc4 = st.columns(4)

            s_total = len(strat_closed)
            s_win = len(strat_closed[strat_closed['result_status'] == 'WIN']) if not strat_closed.empty and 'result_status' in strat_closed.columns else 0
            s_loss = len(strat_closed[strat_closed['result_status'] == 'LOSS']) if not strat_closed.empty and 'result_status' in strat_closed.columns else 0
            s_expired = len(strat_closed[strat_closed['result_status'] == 'EXPIRED']) if not strat_closed.empty and 'result_status' in strat_closed.columns else 0
            s_wr = (s_win / s_total * 100) if s_total > 0 else 0
            s_active = len(strat_open[strat_open['status'].isin(['OPEN', 'PENDING'])]) if not strat_open.empty and 'status' in strat_open.columns else 0

            sc1.metric("총 청산건", f"{s_total}건")
            sc2.metric("승률", f"{s_wr:.1f}%")
            sc3.metric(f"✅ {s_win} / ❌ {s_loss} / ⏰ {s_expired}", "")
            sc4.metric("진행중", f"{s_active}건")

            if not strat_closed.empty and 'result_pct' in strat_closed.columns:
                s_avg_ret = pd.to_numeric(strat_closed['result_pct'], errors='coerce').mean()
                s_max_ret = pd.to_numeric(strat_closed['result_pct'], errors='coerce').max()
                s_min_ret = pd.to_numeric(strat_closed['result_pct'], errors='coerce').min()

                sr1, sr2, sr3 = st.columns(3)
                sr1.metric("평균 수익률", f"{s_avg_ret:+.2f}%" if not pd.isna(s_avg_ret) else "—")
                sr2.metric("최대 수익", f"{s_max_ret:+.2f}%" if not pd.isna(s_max_ret) else "—")
                sr3.metric("최대 손실", f"{s_min_ret:+.2f}%" if not pd.isna(s_min_ret) else "—")

                # 평균 달성률 (미달성 건만)
                non_win = strat_closed[strat_closed['result_status'] != 'WIN']
                if not non_win.empty and 'max_achievement_pct' in non_win.columns:
                    avg_ach = pd.to_numeric(non_win['max_achievement_pct'], errors='coerce').mean()
                    if not pd.isna(avg_ach):
                        st.info(f"📊 미달성 건의 평균 목표 달성률: {avg_ach:.1f}% (목표 +{STRATEGY_TP.get(selected_strat, 0)}%의 {avg_ach:.1f}%까지 도달)")

                # 평균 보유일
                if 'days_held' in strat_closed.columns:
                    win_days = pd.to_numeric(strat_closed[strat_closed['result_status'] == 'WIN']['days_held'], errors='coerce').mean()
                    if not pd.isna(win_days):
                        st.success(f"⏱ 익절 평균 소요일: {win_days:.1f}일")

            st.divider()

            # 해당 전략 포지션 목록
            st.markdown("**청산 완료 포지션**")
            if not strat_closed.empty:
                strat_closed_sorted = strat_closed.sort_values('signal_date', ascending=False) if 'signal_date' in strat_closed.columns else strat_closed
                for _, row in strat_closed_sorted.head(30).iterrows():
                    st.markdown(render_position_card(row, 'closed'), unsafe_allow_html=True)
            else:
                st.markdown('<div class="no-signal">청산 데이터 없음</div>', unsafe_allow_html=True)

            if not strat_open.empty and 'status' in strat_open.columns:
                active_strat = strat_open[strat_open['status'].isin(['OPEN', 'PENDING'])]
                if not active_strat.empty:
                    st.divider()
                    st.markdown("**진행중 포지션**")
                    for _, row in active_strat.iterrows():
                        st.markdown(render_position_card(row, 'open'), unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════
    # 4) 전체 성과 통계
    # ════════════════════════════════════════════════════════════════════
    with h_stats:
        if closed_pos.empty:
            st.markdown('<div class="no-signal">아직 청산 데이터가 없습니다</div>', unsafe_allow_html=True)
        else:
            st.markdown("### 전략별 성과 비교")

            strategies = ['A', 'B', 'C', 'D', 'E']
            perf_rows = []

            for strat in strategies:
                s_closed = closed_pos[closed_pos['strategy'] == strat] if 'strategy' in closed_pos.columns else pd.DataFrame()
                s_open_df = open_pos[(open_pos['strategy'] == strat) & (open_pos['status'].isin(['OPEN', 'PENDING']))] if not open_pos.empty and 'strategy' in open_pos.columns else pd.DataFrame()

                total = len(s_closed)
                wins = len(s_closed[s_closed['result_status'] == 'WIN']) if not s_closed.empty and 'result_status' in s_closed.columns else 0
                losses = len(s_closed[s_closed['result_status'] == 'LOSS']) if not s_closed.empty and 'result_status' in s_closed.columns else 0
                expired = len(s_closed[s_closed['result_status'] == 'EXPIRED']) if not s_closed.empty and 'result_status' in s_closed.columns else 0
                active_count = len(s_open_df)

                wr = (wins / total * 100) if total > 0 else 0

                avg_ret_val = pd.to_numeric(s_closed['result_pct'], errors='coerce').mean() if not s_closed.empty and 'result_pct' in s_closed.columns else 0
                avg_ret_str = f"{avg_ret_val:+.1f}%" if not pd.isna(avg_ret_val) and avg_ret_val != 0 else "—"

                # 평균 익절 소요일
                win_rows = s_closed[s_closed['result_status'] == 'WIN'] if not s_closed.empty and 'result_status' in s_closed.columns else pd.DataFrame()
                avg_win_days = pd.to_numeric(win_rows['days_held'], errors='coerce').mean() if not win_rows.empty and 'days_held' in win_rows.columns else 0
                avg_win_days_str = f"{avg_win_days:.1f}일" if not pd.isna(avg_win_days) and avg_win_days > 0 else "—"

                # 미달성 건 평균 달성률
                non_win = s_closed[s_closed['result_status'] != 'WIN'] if not s_closed.empty and 'result_status' in s_closed.columns else pd.DataFrame()
                avg_ach = pd.to_numeric(non_win['max_achievement_pct'], errors='coerce').mean() if not non_win.empty and 'max_achievement_pct' in non_win.columns else 0
                avg_ach_str = f"{avg_ach:.0f}%" if not pd.isna(avg_ach) and avg_ach > 0 else "—"

                perf_rows.append({
                    '전략': f"{STRATEGY_EMOJI.get(strat, '')} {strat}",
                    '목표': f"+{STRATEGY_TP.get(strat, 0)}%",
                    '청산건': total,
                    '✅ 익절': wins,
                    '❌ 손절': losses,
                    '⏰ 만기': expired,
                    '🔄 진행중': active_count,
                    '실전 승률': f"{wr:.1f}%",
                    '평균수익률': avg_ret_str,
                    '익절소요일': avg_win_days_str,
                    '미달성 도달률': avg_ach_str,
                })

            perf_df = pd.DataFrame(perf_rows)
            st.dataframe(perf_df, use_container_width=True, hide_index=True, height=250)

            st.divider()

            # 전체 요약
            st.markdown("### 전체 요약")
            tot1, tot2, tot3, tot4 = st.columns(4)
            tot1.metric("총 청산건", f"{total_closed}건")
            tot2.metric("총 익절건", f"{total_win}건")
            tot3.metric("전체 승률", f"{overall_wr:.1f}%")
            tot4.metric("전체 평균수익률", f"{avg_return:+.1f}%")

            # 월별 추이 (간단 테이블)
            if 'signal_date' in closed_pos.columns:
                closed_pos_copy = closed_pos.copy()
                closed_pos_copy['month'] = pd.to_datetime(closed_pos_copy['signal_date'], errors='coerce').dt.strftime('%Y-%m')
                closed_pos_copy = closed_pos_copy.dropna(subset=['month'])

                if not closed_pos_copy.empty:
                    st.divider()
                    st.markdown("### 월별 추이")

                    month_data = []
                    for month in sorted(closed_pos_copy['month'].unique()):
                        m_df = closed_pos_copy[closed_pos_copy['month'] == month]
                        m_total = len(m_df)
                        m_win = len(m_df[m_df['result_status'] == 'WIN'])
                        m_wr = (m_win / m_total * 100) if m_total > 0 else 0
                        m_ret = pd.to_numeric(m_df['result_pct'], errors='coerce').mean()
                        month_data.append({
                            '월': month,
                            '건수': m_total,
                            '익절': m_win,
                            '승률': f"{m_wr:.0f}%",
                            '평균수익률': f"{m_ret:+.1f}%" if not pd.isna(m_ret) else "—",
                        })

                    month_df = pd.DataFrame(month_data)
                    st.dataframe(month_df, use_container_width=True, hide_index=True)


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
    tracker_info = load_tracker_summary()
    if tracker_info:
        st.markdown(f"**마지막 추적:** {tracker_info.get('last_tracked', 'N/A')}")
        st.markdown(f"진행중: {tracker_info.get('open_count', 0)}건 | "
                   f"대기: {tracker_info.get('pending_count', 0)}건 | "
                   f"청산: {tracker_info.get('closed_count', 0)}건")

    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()
