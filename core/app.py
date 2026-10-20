#!/usr/bin/env python3
"""
Surge Scanner 대시보드 (v6 UI — 네 화면)
- 오늘: 마감 전 신호(살 것) · 이번 장 마감에 정리할 것 · 지금 할 일
- 보유: 산 뒤 아직 안 판 것
- 성적: 100만 원 시뮬레이션(15:30에 잡힌 신호만) · 전략별 성적
- 기록: 끝난 거래 · 최근 신호 · 규칙
모든 숫자는 '실제로 사고팔 수 있었던 가격' 기준(v6 추적 규칙, 비용 포함). 종목 추천이 아니라 기록·검증용.
실행: streamlit run app.py
"""
import os, json, glob
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

try:
    from shared_config import STRATEGY_CONFIG, STRATEGY_NAMES, ACTIVE_STRATEGIES, COST_PCT, KST, is_us_dst
except Exception:  # 단독 실행 대비
    KST = timezone(timedelta(hours=9))
    ACTIVE_STRATEGIES = ['1', '3']; COST_PCT = 1.0
    STRATEGY_CONFIG = {s: {'tp_pct': 0.05, 'sl_pct': -0.10, 'max_hold': 2} for s in ACTIVE_STRATEGIES}
    STRATEGY_NAMES = {'1': '1차_1 · 급락반등', '3': '1차_3 · 과매도'}
    def is_us_dst(d):
        return 3 < d.month < 11 or (d.month == 3 and d.day >= 8 + (6 - datetime(d.year, 3, 1).weekday()) % 7) or (d.month == 11 and d.day < 1 + (6 - datetime(d.year, 11, 1).weekday()) % 7)

st.set_page_config(page_title="Surge Scanner", page_icon="📈", layout="wide", initial_sidebar_state="collapsed")

# ─── 색 (dataviz 기준 팔레트: 어두운 바탕용) ─────────────────────────────────
C = dict(bg="#141413", surface="#1f1f1d", line="#2e2e2b", text="#f0efec", sub="#c3c2b7", muted="#8a897f",
         blue="#3987e5", good="#0ca30c", warn="#fab219", bad="#d03b3b", red="#e66767")
STRAT_SHORT = {'1': '1차_1', '3': '1차_3'}
STRAT_KR = {'1': '급락반등', '3': '과매도'}
METHOD_V6 = 'v6_close_entry'

st.markdown(f"""
<style>
:root {{ color-scheme: dark; }}
header[data-testid="stHeader"] {{ background:transparent; height:0; }}
#MainMenu, footer, .stDeployButton, .stAppDeployButton, [data-testid="stAppDeployButton"], [data-testid="stToolbar"] {{ display:none; }}
.grid {{ display:grid; gap:10px; grid-template-columns:repeat(auto-fill, minmax(min(100%, 300px), 1fr)); }}
.grid2 {{ display:grid; gap:8px; grid-template-columns:1fr 1fr; }}
.kpi.hero {{ grid-column:1 / -1; }}
.row {{ display:flex; justify-content:space-between; align-items:center; gap:8px; }}
.chips {{ display:flex; flex-wrap:wrap; gap:6px 12px; color:{C['sub']}; font-size:0.84rem; margin-top:8px; }}
.chips b {{ color:{C['text']}; font-weight:600; }}
@media (max-width:640px) {{
  .block-container {{ padding-left:0.9rem; padding-right:0.9rem; padding-top:1.0rem; }}
  .ss-h1 {{ font-size:1.3rem; }}
  .kpi {{ padding:12px 13px; min-height:0; }}
  .kpi .v {{ font-size:1.35rem; }}
  .kpi.hero .v {{ font-size:1.8rem; }}
  .ss-sec {{ margin:18px 0 8px; }}
}}
.stApp {{ background:{C['bg']}; color:{C['text']}; }}
.block-container {{ padding-top:2.0rem; padding-bottom:3rem; max-width:1180px; }}
h1,h2,h3 {{ color:{C['text']} !important; letter-spacing:-0.01em; }}
.ss-h1 {{ font-size:1.6rem; font-weight:700; margin:0; }}
.ss-sub {{ color:{C['sub']}; font-size:0.92rem; margin-top:2px; }}
.ss-sec {{ font-size:1.1rem; font-weight:700; margin:22px 0 10px; display:flex; align-items:baseline; gap:10px; }}
.ss-sec small {{ color:{C['muted']}; font-weight:400; font-size:0.85rem; }}
.kpi {{ background:{C['surface']}; border:1px solid {C['line']}; border-radius:12px; padding:14px 16px; min-height:92px; }}
.kpi .l {{ color:{C['sub']}; font-size:0.82rem; }}
.kpi .v {{ font-size:1.7rem; font-weight:700; font-variant-numeric:tabular-nums; margin-top:2px; }}
.kpi .d {{ color:{C['muted']}; font-size:0.78rem; margin-top:2px; }}
.card {{ background:{C['surface']}; border:1px solid {C['line']}; border-radius:12px; padding:14px 16px; margin-bottom:10px; }}
.card.act {{ border-color:{C['blue']}88; }}
.card.sell {{ border-color:{C['warn']}88; }}
.tk {{ font-size:1.25rem; font-weight:700; letter-spacing:0.02em; }}
.badge {{ display:inline-block; white-space:nowrap; padding:2px 8px; border-radius:999px; font-size:0.74rem; font-weight:600; border:1px solid {C['line']}; color:{C['sub']}; margin-right:4px; vertical-align:middle; }}
.b1 {{ border-color:{C['blue']}66; color:{C['blue']}; }}
.b3 {{ border-color:{C['warn']}66; color:{C['warn']}; }}
.pill {{ display:inline-block; padding:3px 10px; border-radius:6px; font-weight:700; font-size:0.85rem; }}
.win {{ background:{C['good']}22; color:#5fd35f; }}
.loss {{ background:{C['bad']}22; color:{C['red']}; }}
.exp {{ background:{C['warn']}22; color:{C['warn']}; }}
.open {{ background:{C['blue']}22; color:{C['blue']}; }}
.bar {{ position:relative; height:10px; background:{C['line']}; border-radius:5px; margin:8px 0 4px; }}
.bar .z {{ position:absolute; left:66.7%; top:-3px; width:2px; height:16px; background:{C['sub']}; }}
.bar .f {{ position:absolute; top:0; height:10px; border-radius:5px; }}
.bar .m {{ position:absolute; top:-3px; width:4px; height:16px; border-radius:2px; background:{C['text']}; }}
.note {{ background:{C['surface']}; border-left:4px solid {C['warn']}; padding:10px 14px; border-radius:6px; color:{C['sub']}; font-size:0.9rem; }}
.todo {{ background:{C['surface']}; border-left:4px solid {C['blue']}; padding:10px 14px; border-radius:6px; color:{C['text']}; font-size:0.92rem; line-height:1.55; }}
.todo b {{ color:{C['text']}; }}
.muted {{ color:{C['muted']}; font-size:0.85rem; }}
div[data-testid="stExpander"] {{ border:1px solid {C['line']}; border-radius:10px; background:{C['surface']}; }}
div[data-testid="stSegmentedControl"] button, div[data-testid="stSegmentedControl"] label {{ font-weight:700; }}
</style>
""", unsafe_allow_html=True)


# ─── 데이터 ──────────────────────────────────────────────────────────────────
def _csv(path, dtype=str):
    if os.path.exists(path):
        try:
            return pd.read_csv(path, dtype=dtype, keep_default_na=False, na_filter=False)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _num(s):
    return pd.to_numeric(s, errors='coerce')


@st.cache_data(ttl=60)
def load_all():
    d = {}
    d['scan'] = json.load(open('data/latest_scan.json')) if os.path.exists('data/latest_scan.json') else {}
    d['summary'] = json.load(open('data/tracker_summary.json')) if os.path.exists('data/tracker_summary.json') else {}
    files = sorted(glob.glob('data/signal_*.csv'))
    d['signal_file'] = files[-1] if files else None
    d['today'] = _csv(files[-1]) if files else pd.DataFrame()
    d['open'] = _csv('data/open_positions.csv')
    d['closed'] = _csv('data/closed_positions.csv')
    d['history'] = _csv('data/history.csv')
    d['backfill'] = _csv('data/preclose_backfill.csv')
    for k in ('today', 'open', 'closed', 'history'):
        df = d[k]
        if not df.empty and 'strategy' in df.columns:
            df['strategy'] = df['strategy'].astype(str)
            d[k] = df[df['strategy'].isin(ACTIVE_STRATEGIES)].reset_index(drop=True)
    return d


D = load_all()
today, open_pos, closed, history, backfill = D['today'], D['open'], D['closed'], D['history'], D['backfill']
scan, summary = D['scan'], D['summary']

for df in (open_pos, closed):
    for c in ('entry_price', 'current_price', 'change_pct', 'tp_price', 'sl_price', 'days_held', 'max_hold',
              'result_pct', 'gross_pct', 'overnight_pct', 'signal_price', 'close_price', 'max_price', 'min_price'):
        if c in df.columns:
            df[c] = _num(df[c])
has_v6 = 'method' in closed.columns and (closed['method'] == METHOD_V6).any()
closed_v6 = closed[closed['method'] == METHOD_V6] if has_v6 else closed


def mark_executable(df):
    """실제로 살 수 있었던 신호인지 표시.
    exec_ok: 마감 전(preclose) 신호이거나, 예전 기록 중 15:30 분석(data/preclose_backfill.csv)에서 조건이 이미 맞았던 것. ETF 는 제외.
    is_etf: 레버리지 ETF 등 (예전 기록에만 섞여 있음 — 지금은 스캔에서 뺌)"""
    df = df.copy()
    st_ = df['scan_type'].astype(str) if 'scan_type' in df.columns else pd.Series('', index=df.index)
    exec_ok = st_.eq('preclose').values.copy()
    is_etf = np.zeros(len(df), dtype=bool)
    if not backfill.empty and {'ticker', 'signal_date', 'flag1530', 'etf'} <= set(backfill.columns):
        bf = backfill.drop_duplicates(['ticker', 'signal_date']).set_index(['ticker', 'signal_date'])
        legacy = ~st_.isin(['preclose', 'afterclose']).values
        for i, k in enumerate(zip(df['ticker'], df['signal_date'])):
            if k in bf.index:
                is_etf[i] = bf.at[k, 'etf'] == 'True'
                if legacy[i]:
                    exec_ok[i] = bf.at[k, 'flag1530'] == 'True'
    exec_ok &= ~is_etf
    df['exec_ok'] = exec_ok; df['is_etf'] = is_etf
    return df


# ─── 유틸 ────────────────────────────────────────────────────────────────────
def pill(status):
    m = {'WIN': ('win', '익절'), 'LOSS': ('loss', '손절'), 'EXPIRED': ('exp', '만기'), 'OPEN': ('open', '진행'), 'PENDING': ('open', '대기')}
    cls, txt = m.get(status, ('exp', status or '—'))
    return f'<span class="pill {cls}">{txt}</span>'


def badges(strats, short=False):
    return ''.join(f'<span class="badge b{s}">{STRAT_SHORT.get(s, s)}{"" if short else " " + STRAT_KR.get(s, "")}</span>' for s in sorted(set(strats)))


def fmt_pct(x, plus=True):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return '—'
    return f"{x:+.1f}%" if plus else f"{x:.1f}%"


def color_pct(x):
    if x is None or np.isnan(x):
        return C['muted']
    return '#5fd35f' if x > 0 else (C['red'] if x < 0 else C['sub'])


def simulate(trades, cap=1_000_000, slots=10):
    """100만 원 · 한 종목 10% · 최대 10종목. trades: in,out,r(소수)"""
    if trades.empty:
        return pd.DataFrame(), cap, 0.0
    T = trades.sort_values('in'); cash = cap; op = []; curve = []; peak = cap; mdd = 0.0
    for dd in sorted(set(T['in']) | set(T['out'])):
        for p in [p for p in op if p[0] <= dd]:
            cash += p[1]; op.remove(p)
        eq = cash + sum(p[2] for p in op)
        for t in T[T['in'] == dd].itertuples():
            if len(op) >= slots:
                continue
            sz = min(cash, eq / slots)
            if sz <= 0:
                continue
            cash -= sz; op.append((t.out, sz * (1 + t.r), sz))
        eq = cash + sum(p[2] for p in op); peak = max(peak, eq); mdd = min(mdd, eq / peak - 1)
        curve.append((dd, eq))
    final = cash + sum(p[1] for p in op)
    return pd.DataFrame(curve, columns=['date', 'equity']), final, mdd


def sim_of(df):
    """끝난 거래 → (같은 날 같은 종목 1회) 시뮬 결과와 기본 통계"""
    cl = df[df['result_pct'].notna()] if 'result_pct' in df.columns else df.iloc[0:0]
    uniq = cl.sort_values('strategy').drop_duplicates(['ticker', 'signal_date']) if not cl.empty else cl
    trades = pd.DataFrame()
    if not uniq.empty:
        trades = pd.DataFrame({'in': pd.to_datetime(uniq['entry_date'], errors='coerce'), 'out': pd.to_datetime(uniq['close_date'], errors='coerce'),
                               'r': uniq['result_pct'] / 100}).dropna()
    curve, final, mdd = simulate(trades)
    n = len(uniq)
    return dict(uniq=uniq, curve=curve, final=final, mdd=mdd, n=n,
                wr=(uniq['result_status'] == 'WIN').mean() * 100 if n else np.nan, avg=uniq['result_pct'].mean() if n else np.nan,
                top5=uniq['result_pct'].nlargest(5).sum() / 10 if n >= 5 else 0.0)


# 한국 시각 안내 (미국 서머타임이면 1시간 빠름)
_now_kst = datetime.now(KST)
_dst = is_us_dst((_now_kst - timedelta(hours=14)).date())
T_SIG, T_MOC, T_CLOSE, T_TRK = ('04:47', '04:50', '05:00', '05:25') if _dst else ('05:47', '05:50', '06:00', '06:25')

# ─── 헤더 + 화면 선택 ─────────────────────────────────────────────────────────
scan_time = scan.get('scan_time', '—')
sig_date = os.path.basename(D['signal_file']).replace('signal_', '').replace('.csv', '') if D['signal_file'] else '—'
stale = ''
try:
    age = (_now_kst - datetime.strptime(scan_time[:16], '%Y-%m-%d %H:%M').replace(tzinfo=KST)).days
    if age >= 4:
        stale = f'<span class="badge" style="border-color:{C["warn"]};color:{C["warn"]}">스캔이 {age}일 전</span>'
except Exception:
    pass
c1, c2 = st.columns([5, 1])
c1.markdown(f'<div class="ss-h1">Surge Scanner</div><div class="ss-sub">마감 전 탐지 → 마감가 매수 · +5% / −10% / 2일 · 마지막 스캔 {scan_time} {stale}</div>', unsafe_allow_html=True)
if c2.button("새로고침", help="60초 캐시를 비우고 다시 읽어요", use_container_width=True):
    st.cache_data.clear(); st.rerun()

VIEWS = ['오늘', '보유', '성적', '기록']
try:
    view = st.segmented_control('화면', VIEWS, default='오늘', selection_mode='single', label_visibility='collapsed', key='view') or '오늘'
except Exception:
    view = st.radio('화면', VIEWS, horizontal=True, label_visibility='collapsed', key='view_radio')

# 공통으로 쓰는 것
act = open_pos[open_pos['status'].isin(['OPEN', 'PENDING'])] if not open_pos.empty and 'status' in open_pos else pd.DataFrame()
n_open = int((open_pos['status'] == 'OPEN').sum()) if not open_pos.empty and 'status' in open_pos else 0
closed_m = mark_executable(closed_v6) if not closed_v6.empty else closed_v6


# ════════════════════════════════════════════════════════════════════════════
if view == '오늘':
    # ── 오늘 신호 ──
    pre_rows = today[today['scan_type'] == 'preclose'] if (not today.empty and 'scan_type' in today.columns) else pd.DataFrame()
    pre_ran = scan.get('mode') == 'preclose' and scan.get('scan_date') == sig_date
    st.markdown(f'<div class="ss-sec">오늘 신호 <small>{sig_date} 미국장 · 마감 전 스캔 기준</small></div>', unsafe_allow_html=True)
    if pre_rows.empty:
        if pre_ran or (not today.empty and 'scan_type' in today.columns):
            st.markdown(f'<div class="card muted">마감 전 스캔에서 조건에 맞는 종목이 없었어요. 오늘은 사지 않아요.</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="card muted">마감 전 스캔은 매일 {T_SIG}(한국)에 올라와요. 아직 오늘 결과가 없어요.</div>', unsafe_allow_html=True)
    else:
        g = pre_rows.groupby('ticker')
        rows = []
        for tk, x in g:
            r0 = x.iloc[0]
            rows.append(dict(ticker=tk, strats=list(x['strategy']), price=_num(pd.Series([r0.get('price')])).iloc[0],
                             rsi=_num(pd.Series([r0.get('rsi7')])).iloc[0], intra=_num(pd.Series([r0.get('intraday')])).iloc[0],
                             ret1=_num(pd.Series([r0.get('ret1d')])).iloc[0], down=r0.get('consec_down', ''), vol=_num(pd.Series([r0.get('avg_vol20', np.nan)])).iloc[0]))
        rows.sort(key=lambda r: (-len(r['strats']), r['ticker']))
        st.markdown(f'<div class="todo"><b>{len(rows)}종목.</b> 각각 투자금의 10%, 동시에 최대 10종목. <b>{T_MOC}까지 종가 주문(MOC)</b>, 안 되면 {T_CLOSE} 직전 시장가. {T_CLOSE} 넘기면 사지 않아요.</div>', unsafe_allow_html=True)
        cards = ''
        for r in rows:
            p = r['price']
            down = int(float(r['down'])) if str(r['down']).replace('.', '', 1).isdigit() else r['down']
            cards += (f'<div class="card act"><div class="row"><span class="tk">{r["ticker"]}</span><span style="font-size:1.1rem;font-weight:700">${p:,.2f} <span class="muted" style="font-size:0.75rem">15:30</span></span></div>'
                      f'<div style="margin-top:4px">{badges(r["strats"])}</div>'
                      f'<div class="chips"><span>익절 <b style="color:#5fd35f">${p*1.05:,.2f}</b></span><span>손절 <b style="color:{C["red"]}">${p*0.9:,.2f}</b></span>'
                      f'<span>RSI7 <b>{r["rsi"]:.0f}</b></span><span>일중 <b>{r["intra"]:.0f}%</b></span><span>당일 <b>{fmt_pct(r["ret1"])}</b></span><span>연속하락 <b>{down}일</b></span></div></div>')
        st.markdown(f'<div class="grid">{cards}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="muted">가격은 15:30 ET 기준. 실제 진입가(종가)와 그에 맞춘 익절·손절가는 {T_TRK} 추적 뒤 "보유" 화면에 확정돼요.</div>', unsafe_allow_html=True)

    # ── 이번 장 마감에 정리할 것 ──
    due = pd.DataFrame()
    if not act.empty:
        a2 = act[act['status'] == 'OPEN'].copy()
        if not a2.empty:
            rem = a2['max_hold'].fillna(2) - a2['days_held'].fillna(0)
            due = a2[rem <= 1].drop_duplicates('ticker')
    st.markdown('<div class="ss-sec">이번 장 마감에 정리 <small>2거래일이 끝나는 것</small></div>', unsafe_allow_html=True)
    if due.empty:
        st.markdown('<div class="card muted">없어요. 보유 중인 건 걸어 둔 익절·손절 주문이 알아서 처리해요.</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="todo">아래 종목은 이번 장이 마지막(2일째)이에요. 익절·손절이 안 됐으면 <b>{T_MOC} 종가 주문으로 전량 매도</b>하고, 걸어 둔 주문은 취소해요.</div>', unsafe_allow_html=True)
        cards = ''
        for r in due.itertuples():
            ch = r.change_pct if not np.isnan(r.change_pct) else 0.0
            cards += (f'<div class="card sell"><div class="row"><span class="tk">{r.ticker}</span><span style="font-weight:700;color:{color_pct(ch)}">{fmt_pct(ch)}</span></div>'
                      f'<div class="row muted" style="margin-top:4px"><span>진입 ${r.entry_price:,.2f} · 현재 ${r.current_price:,.2f}</span><span>{int(r.days_held) if not np.isnan(r.days_held) else 0}/{int(r.max_hold) if not np.isnan(r.max_hold) else 2}일</span></div></div>')
        st.markdown(f'<div class="grid">{cards}</div>', unsafe_allow_html=True)

    # ── 오늘 할 일 요약 ──
    st.markdown('<div class="ss-sec">매일 순서</div>', unsafe_allow_html=True)
    st.markdown(f"""<div class="card muted" style="line-height:1.6">
<b style="color:{C['text']}">{T_SIG}</b> 신호 확인 → <b style="color:{C['text']}">{T_MOC}</b> 정리할 것 먼저 종가 매도, 새 신호 종가 매수 → <b style="color:{C['text']}">{T_CLOSE}</b> 마감 →
<b style="color:{C['text']}">{T_TRK}</b> "보유" 화면에서 진입가·목표가·손절가 확인하고 매도 주문 두 개(목표가 지정가, 손절가 스탑) 걸기. 낮에는 할 일 없음.
<br>일·월 새벽은 미국 장이 없어요. {'11월 첫째 일요일부터 겨울 시간이라 한 시간 늦어져요.' if _dst else '3월 둘째 일요일부터 서머타임이라 한 시간 빨라져요.'}</div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
elif view == '보유':
    st.markdown(f'<div class="ss-sec">보유 <small>{n_open}건 진행 중</small></div>', unsafe_allow_html=True)
    if act.empty:
        st.markdown('<div class="card muted">진행 중인 포지션이 없어요.</div>', unsafe_allow_html=True)
    else:
        a2 = act.sort_values(['status', 'signal_date'], ascending=[False, False])
        cards = ''; fresh = []
        for tk, x in a2.groupby('ticker', sort=False):
            r = x.iloc[0]
            if r['status'] == 'OPEN' and (np.isnan(r.get('days_held', np.nan)) or r.get('days_held', 0) < 1):
                fresh.append((tk, list(x['strategy']), r.get('entry_price'), r.get('tp_price'), r.get('sl_price')))
                continue
            ep, cp, ch = r.get('entry_price'), r.get('current_price'), r.get('change_pct')
            if np.isnan(ch) and not np.isnan(ep):
                ch = 0.0; cp = ep if np.isnan(cp) else cp
            dh, mh = r.get('days_held'), r.get('max_hold') if not np.isnan(r.get('max_hold', np.nan)) else 2
            if r['status'] == 'PENDING' or np.isnan(ep):
                body = f'<div class="muted">신호일 마감가로 진입 예정(종가 확정 대기) · 신호 {r["signal_date"]}</div>'
            else:
                pos = min(max((ch + 10) / 15, 0), 1) * 100 if not np.isnan(ch) else 0
                fill = '#5fd35f' if ch >= 0 else C['red']
                left, width = (66.7, pos - 66.7) if pos >= 66.7 else (pos, 66.7 - pos)
                body = (f'<div style="display:flex;justify-content:space-between;margin-top:4px"><span class="muted">진입 ${ep:,.2f}</span>'
                        f'<span style="font-weight:700;color:{color_pct(ch)}">{fmt_pct(ch)}</span><span class="muted">현재 ${cp:,.2f}</span></div>'
                        f'<div class="bar"><div class="f" style="left:{left}%;width:{max(width,0)}%;background:{fill}"></div><div class="z"></div><div class="m" style="left:calc({pos}% - 2px)"></div></div>'
                        f'<div style="display:flex;justify-content:space-between" class="muted"><span>손절 ${r["sl_price"]:,.2f}</span><span>{int(dh) if not np.isnan(dh) else 0}/{int(mh)}일</span><span>목표 ${r["tp_price"]:,.2f}</span></div>')
            cards += (f'<div class="card"><div class="row"><span class="tk">{tk}</span>{pill(r["status"])}</div>'
                      f'<div style="margin:4px 0">{badges(x["strategy"])}</div>{body}</div>')
        if fresh:
            st.markdown(f'<div class="todo">방금 들어간 {len(fresh)}종목 — 아래 값으로 <b>매도 주문 두 개</b>를 걸어 두세요.</div>', unsafe_allow_html=True)
            st.markdown('<div class="grid">' + ''.join(
                f'<div class="card act"><div class="row"><span class="tk">{tk}</span>{badges(ss, short=True)}</div>'
                f'<div class="chips"><span>진입 <b>${ep:,.2f}</b></span><span>목표가 지정가 매도 <b style="color:#5fd35f">${tp:,.2f}</b></span><span>손절가 스탑 매도 <b style="color:{C["red"]}">${sl:,.2f}</b></span></div></div>'
                for tk, ss, ep, tp, sl in fresh) + '</div>', unsafe_allow_html=True)
        if cards:
            st.markdown(f'<div class="grid">{cards}</div>', unsafe_allow_html=True)
        st.markdown('<div class="muted" style="margin-top:6px">막대: 손절(−10%) ─ 0 ─ 목표(+5%). 세로선이 0, 흰 점이 지금 위치. 현재가는 마지막 추적 시점 기준이에요.</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
elif view == '성적':
    ex = closed_m[closed_m['exec_ok']] if not closed_m.empty else closed_m
    allv = closed_m[~closed_m['is_etf']] if not closed_m.empty else closed_m
    S = sim_of(ex); A = sim_of(allv)
    ret = (S['final'] / 1_000_000 - 1) * 100 if S['n'] else np.nan
    st.markdown('<div class="ss-sec">성적 <small>살 수 있었던 신호만 · 비용 포함</small></div>', unsafe_allow_html=True)
    kpis = [
        ('hero', '100만 원으로 따라 샀다면', f"{S['final']:,.0f}원" if S['n'] else '—',
         f"{fmt_pct(ret)} · 15:30에 잡힌 신호 {S['n']}건 · 한 종목 10% · 최대 10종목 · 상위 5건 빼면 {S['final'] - S['top5'] * 10_000 * 1:,.0f}원쯤" if S['n'] else '아직 없음', color_pct(ret)),
        ('', '중간에 가장 많이 줄었을 때', fmt_pct(S['mdd'] * 100) if S['n'] else '—', '최고점 대비', C['red'] if S['n'] else C['muted']),
        ('', '+5% 도달률', fmt_pct(S['wr'], False) if S['n'] else '—', f"{S['n']}건", C['text']),
        ('', '한 번 살 때 평균', fmt_pct(S['avg']) if S['n'] else '—', '비용 뺀 값', color_pct(S['avg'])),
        ('', '지금 들고 있는 것', f"{n_open}건", '진행 중', C['text']),
    ]
    st.markdown('<div class="grid2">' + ''.join(
        f'<div class="kpi {cls}"><div class="l">{l}</div><div class="v" style="color:{vc}">{v}</div><div class="d">{dsc}</div></div>'
        for cls, l, v, dsc, vc in kpis) + '</div>', unsafe_allow_html=True)
    if not has_v6:
        st.markdown(f'<div class="note" style="margin-top:10px">아직 예전 규칙으로 계산된 기록이에요. 다음 추적 때 전부 새 규칙으로 다시 계산돼요.</div>', unsafe_allow_html=True)
    _u = closed_m.drop_duplicates(['ticker', 'signal_date']) if not closed_m.empty else closed_m
    n_etf = int(_u['is_etf'].sum()) if not _u.empty else 0
    n_late = int(((~_u['exec_ok']) & (~_u['is_etf'])).sum()) if not _u.empty else 0
    st.markdown(f'<div class="muted" style="margin-top:8px">위 숫자는 마감 30분 전에 이미 조건이 맞아서 <b>마감가에 살 수 있었던 신호</b>만 센 거예요. '
                f'마감 뒤에야 잡힌 신호 {n_late}건은 뺐고(그 가격에 살 수 없었음), 레버리지 ETF {n_etf}건도 뺐어요. '
                f'참고로 마감 뒤 신호까지 전부 샀다고 치면 {A["final"]:,.0f}원({A["n"]}건).</div>', unsafe_allow_html=True)

    if S['n'] == 0:
        st.markdown('<div class="card muted" style="margin-top:10px">아직 끝난 거래가 없어요.</div>', unsafe_allow_html=True)
    else:
        uniq, curve = S['uniq'], S['curve']
        st.markdown('<div class="muted" style="margin-top:14px">100만 원으로 시작했을 때 잔고</div>', unsafe_allow_html=True)
        base = alt.Chart(curve).encode(x=alt.X('date:T', title=None, axis=alt.Axis(format='%m/%d', labelColor=C['sub'], gridColor=C['line'], domainColor=C['line'])))
        line = base.mark_line(color=C['blue'], strokeWidth=2).encode(y=alt.Y('equity:Q', title=None, scale=alt.Scale(zero=False), axis=alt.Axis(format=',.0f', labelColor=C['sub'], gridColor=C['line'], domainColor=C['line'])))
        rule = alt.Chart(pd.DataFrame({'y': [1_000_000]})).mark_rule(color=C['muted'], strokeDash=[4, 4]).encode(y='y:Q')
        hover = alt.selection_point(fields=['date'], nearest=True, on='mouseover', empty=False)
        pts = base.mark_circle(size=60, color=C['blue']).encode(y='equity:Q', opacity=alt.condition(hover, alt.value(1), alt.value(0)),
                                                                tooltip=[alt.Tooltip('date:T', title='날짜', format='%Y-%m-%d'), alt.Tooltip('equity:Q', title='잔고', format=',.0f')]).add_params(hover)
        st.altair_chart((rule + line + pts).properties(height=200, background=C['surface']).configure_view(stroke=None), use_container_width=True)

        st.markdown('<div class="muted">월별 평균 손익 (건당 %)</div>', unsafe_allow_html=True)
        m = uniq.assign(month=pd.to_datetime(uniq['signal_date'], errors='coerce').dt.strftime('%y-%m')).groupby('month').agg(평균=('result_pct', 'mean'), 건수=('result_pct', 'size')).reset_index()
        m['sign'] = np.where(m['평균'] >= 0, '플러스', '마이너스')
        bar = alt.Chart(m).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, size=22).encode(
            x=alt.X('month:N', title=None, axis=alt.Axis(labelAngle=0, labelColor=C['sub'], domainColor=C['line'])),
            y=alt.Y('평균:Q', title=None, axis=alt.Axis(labelColor=C['sub'], gridColor=C['line'], domainColor=C['line'])),
            color=alt.Color('sign:N', scale=alt.Scale(domain=['플러스', '마이너스'], range=[C['blue'], C['red']]), legend=None),
            tooltip=[alt.Tooltip('month:N', title='월'), alt.Tooltip('평균:Q', title='평균 %', format='+.2f'), alt.Tooltip('건수:Q', title='건수')])
        st.altair_chart(bar.properties(height=160, background=C['surface']).configure_view(stroke=None), use_container_width=True)

        st.markdown('<div class="ss-sec">전략별 <small>살 수 있었던 신호 기준</small></div>', unsafe_allow_html=True)
        cards = ''
        for s in ACTIVE_STRATEGIES:
            x = ex[ex['strategy'] == s]
            if x.empty:
                continue
            on = x['overnight_pct'].dropna() if 'overnight_pct' in x else pd.Series(dtype=float)
            w, l, e = (x['result_status'] == 'WIN').sum(), (x['result_status'] == 'LOSS').sum(), (x['result_status'] == 'EXPIRED').sum()
            cards += (f'<div class="card"><div class="row"><span>{badges([s])}</span><span class="muted">{len(x)}건</span></div>'
                      f'<div class="row" style="margin-top:8px"><div><div class="muted">건당 평균</div><div style="font-size:1.3rem;font-weight:700;color:{color_pct(x["result_pct"].mean())}">{fmt_pct(x["result_pct"].mean())}</div></div>'
                      f'<div><div class="muted">도달률</div><div style="font-size:1.3rem;font-weight:700">{(x["result_status"] == "WIN").mean() * 100:.0f}%</div></div>'
                      f'<div><div class="muted">중간값</div><div style="font-size:1.3rem;font-weight:700">{fmt_pct(x["result_pct"].median())}</div></div></div>'
                      f'<div class="chips"><span><b style="color:#5fd35f">익절 {w}</b></span><span><b style="color:{C["red"]}">손절 {l}</b></span><span><b style="color:{C["warn"]}">만기 {e}</b></span><span>하룻밤 실험 <b>{fmt_pct(on.mean() if len(on) else np.nan)}</b></span></div></div>')
        st.markdown(f'<div class="grid">{cards}</div>', unsafe_allow_html=True)
        nj = ex['judge'].value_counts().to_dict() if 'judge' in ex.columns else {}
        st.markdown(f'<div class="muted" style="margin-top:6px">이기면 +5%, 지면 −10%(갭이면 더 크게)라서 도달률 67%가 넘어야 남아요. 판정: 1분봉 {nj.get("minute", 0)}건 · 일봉 {nj.get("daily", 0)}건. '
                    f'하룻밤 실험 = 신호일 종가에 사서 다음날 시가에 팔았다면(기록용).</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
else:
    st.markdown(f'<div class="ss-sec">끝난 거래 <small>{len(closed)}건</small></div>', unsafe_allow_html=True)
    if closed.empty:
        st.markdown('<div class="muted">없음</div>', unsafe_allow_html=True)
    else:
        f1, f2 = st.columns([1, 3])
        pick = f1.selectbox('전략', ['전체'] + [f"{STRAT_SHORT[s]} {STRAT_KR[s]}" for s in ACTIVE_STRATEGIES], label_visibility='collapsed')
        res = f2.radio('결과', ['전체', '익절', '손절', '만기'], horizontal=True, label_visibility='collapsed')
        x = closed_m if not closed_m.empty else closed.assign(exec_ok=False, is_etf=False)
        if pick != '전체':
            x = x[x['strategy'] == pick.split()[0].replace('1차_', '')]
        if res != '전체':
            x = x[x['result_status'] == {'익절': 'WIN', '손절': 'LOSS', '만기': 'EXPIRED'}[res]]
        x = x.sort_values(['close_date', 'signal_date'], ascending=False).head(300)
        cards = ''
        for r in x.itertuples():
            dh = int(r.days_held) if not np.isnan(r.days_held) else '—'
            tag = '' if r.exec_ok else (f'<span class="badge">ETF</span>' if r.is_etf else f'<span class="badge">마감 뒤 신호</span>')
            cards += (f'<div class="card" style="padding:10px 14px"><div class="row"><span><span class="tk" style="font-size:1.05rem">{r.ticker}</span> '
                      f'<span class="muted">{r.signal_date}</span></span><span style="font-weight:700;color:{color_pct(r.result_pct)}">{fmt_pct(r.result_pct)}</span></div>'
                      f'<div class="row" style="margin-top:4px"><span>{badges([r.strategy], short=True)}{pill(r.result_status)} {tag}</span>'
                      f'<span class="muted">${r.entry_price:,.2f} → ${r.close_price:,.2f} · {dh}일</span></div></div>')
        st.markdown(cards, unsafe_allow_html=True)
        st.markdown('<div class="muted">"마감 뒤 신호" = 마감 뒤 전 종목 스캔에서만 잡혀 그 가격에 살 수 없었던 것, "ETF" = 레버리지 ETF. 둘 다 성적 계산에서는 빼요.</div>', unsafe_allow_html=True)
        st.download_button('CSV로 받기', closed.to_csv(index=False).encode('utf-8-sig'), 'closed_positions.csv', 'text/csv')

    with st.expander("최근 30일 신호 전체", expanded=False):
        if history.empty:
            st.markdown('<div class="muted">없음</div>', unsafe_allow_html=True)
        else:
            hh = history.copy(); hh['date'] = pd.to_datetime(hh['date'], errors='coerce')
            hh = hh[hh['date'] >= (pd.Timestamp.now() - pd.Timedelta(days=30))].sort_values('date', ascending=False)
            daily = hh.groupby(hh['date'].dt.strftime('%m/%d')).agg(종목수=('ticker', 'nunique')).reset_index().sort_values('date')
            st.altair_chart(alt.Chart(daily).mark_bar(color=C['blue'], size=14, cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
                x=alt.X('date:N', title=None, axis=alt.Axis(labelAngle=0, labelColor=C['sub'])), y=alt.Y('종목수:Q', title=None, axis=alt.Axis(labelColor=C['sub'], gridColor=C['line'])),
                tooltip=['date:N', '종목수:Q']).properties(height=140, background=C['surface']).configure_view(stroke=None), use_container_width=True)
            show = hh.assign(전략=hh['strategy'].map(lambda s: f"{STRAT_SHORT.get(s,s)} {STRAT_KR.get(s,'')}"), 날짜=hh['date'].dt.strftime('%Y-%m-%d'),
                             구분=hh['scan_type'].map({'preclose': '마감 전', 'afterclose': '마감 뒤'}).fillna('예전') if 'scan_type' in hh.columns else '예전')
            st.dataframe(show[['날짜', 'ticker', '전략', '구분', 'price', 'rsi7', 'intraday', 'ret1d', 'consec_down']].rename(columns={'ticker': '종목', 'price': '종가', 'rsi7': 'RSI7', 'intraday': '일중변동%', 'ret1d': '당일%', 'consec_down': '연속하락'}),
                         use_container_width=True, hide_index=True, height=360)

    with st.expander("이 화면의 숫자는 어떻게 계산되나 (규칙)", expanded=False):
        st.markdown(f"""
- **감시목록**: 전날 데이터로 "내일 신호가 날 수 있는 종목"(RSI7<45, 연속하락 3일 이상)을 미리 추려요. 하루 400~700종목.
- **신호**: 미국장 마감 30분 전(15:30 ET) 가격으로 감시목록만 확인해서 1차_1·1차_3 조건에 맞는 종목을 올려요(데이터가 15분 늦게 와서 15:46에 돌아요). ETF·레버리지 상품은 빼요. 마감 뒤 전 종목 스캔도 돌지만 그건 기록·대조용이에요.
- **매수**: 신호일 **마감가**(종가 주문)에 산 것으로 계산해요.
- **매도**: +5% 익절, −10% 손절, 최대 2거래일. 1분봉으로 어느 쪽이 먼저였는지 봐요. 아침부터 손절가 아래/익절가 위로 시작하면 그 **시가**로 계산해요.
- **비용**: 거래마다 왕복 {COST_PCT:.0f}%를 빼요.
- **100만 원 시뮬레이션**: 마감 30분 전에 이미 조건이 맞았던 신호만. 한 종목에 그때 잔고의 10%, 동시에 최대 10종목. 자리가 없으면 그 신호는 건너뛰어요.
- **하룻밤 실험**: 신호일 종가에 사서 다음날 시가에 팔았다면의 값. 기록만 해요.
- 2016~2026 과거 재시험에서 두 전략 모두 같은 날 변동 큰 종목을 무작위로 산 경우와 큰 차이가 없었어요. **종목 추천이 아니라 기록·검증용 화면**이에요.
""")
