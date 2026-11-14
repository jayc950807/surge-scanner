#!/usr/bin/env python3
"""
Surge Scanner 대시보드 (v6 UI — 네 화면)
- 오늘: 마감 전 신호(살 것) · 이번 장 마감에 정리할 것 · 지금 할 일
- 보유: 산 뒤 아직 안 판 것
- 성적: 실제 매매(10/27~, 한국투자 체결) / 과거부터 지금까지 100만 원 시뮬레이션(추천 수로 나눠 전부 투자, 15:30에 잡힌 신호만) · 전략별 성적
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
    STRATEGY_CONFIG = {s: {'tp_pct': 0.05, 'sl_pct': -0.20, 'max_hold': 2} for s in ACTIVE_STRATEGIES}
    STRATEGY_NAMES = {'1': '1. 급락반등', '3': '2. 과매도'}
    def is_us_dst(d):
        return 3 < d.month < 11 or (d.month == 3 and d.day >= 8 + (6 - datetime(d.year, 3, 1).weekday()) % 7) or (d.month == 11 and d.day < 1 + (6 - datetime(d.year, 11, 1).weekday()) % 7)

st.set_page_config(page_title="Surge Scanner", page_icon="📈", layout="wide", initial_sidebar_state="collapsed")

# ─── 색 (dataviz 기준 팔레트: 어두운 바탕용) ─────────────────────────────────
C = dict(bg="#141413", surface="#1f1f1d", line="#2e2e2b", text="#f0efec", sub="#c3c2b7", muted="#8a897f",
         blue="#3987e5", good="#0ca30c", warn="#fab219", bad="#d03b3b", red="#e66767")
STRAT_SHORT = {'1': '1.', '3': '2.'}
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
.chips {{ display:flex; flex-wrap:wrap; gap:6px 12px; color:{C['sub']}; font-size:12px; margin-top:8px; }}
.chips b {{ color:{C['text']}; font-weight:600; }}
@media (max-width:640px) {{
  .block-container {{ padding-left:0.9rem; padding-right:0.9rem; padding-top:1.0rem; }}
  .ss-h1 {{ font-size:14px; }}
  .kpi {{ padding:12px 13px; min-height:0; }}
  .kpi .v {{ font-size:14px; }}
  .kpi.hero .v {{ font-size:14px; }}
  .ss-sec {{ margin:18px 0 8px; }}
}}
.stApp {{ background:{C['bg']}; color:{C['text']}; }}
.stApp, .stApp p, .stApp li, .stApp label, .stMarkdown, div[data-testid="stMarkdownContainer"] {{ font-size:12px; letter-spacing:normal; }}
.stApp button p, .stApp button div {{ font-size:12px !important; }}
div[data-testid="stExpander"] summary p {{ font-size:12px !important; }}
.block-container {{ padding-top:2.0rem; padding-bottom:3rem; max-width:1180px; }}
h1,h2,h3 {{ color:{C['text']} !important; letter-spacing:-0.01em; }}
.ss-h1 {{ font-size:14px; font-weight:700; margin:0; }}
.ss-sub {{ color:{C['sub']}; font-size:12px; margin-top:2px; }}
.ss-sec {{ font-size:14px; font-weight:700; margin:22px 0 10px; display:flex; align-items:baseline; gap:10px; }}
.ss-sec small {{ color:{C['muted']}; font-weight:400; font-size:12px; }}
.kpi {{ background:{C['surface']}; border:1px solid {C['line']}; border-radius:12px; padding:14px 16px; min-height:92px; }}
.kpi .l {{ color:{C['sub']}; font-size:12px; }}
.kpi .v {{ font-size:14px; font-weight:700; font-variant-numeric:tabular-nums; margin-top:2px; }}
.kpi .d {{ color:{C['muted']}; font-size:10px; margin-top:2px; }}
.card {{ background:{C['surface']}; border:1px solid {C['line']}; border-radius:12px; padding:14px 16px; margin-bottom:10px; }}
.card.act {{ border-color:{C['blue']}88; }}
.card.sell {{ border-color:{C['warn']}88; }}
.tk {{ font-size:14px; font-weight:700; letter-spacing:0.02em; }}
.badge {{ display:inline-block; white-space:nowrap; padding:2px 8px; border-radius:999px; font-size:10px; font-weight:600; border:1px solid {C['line']}; color:{C['sub']}; margin-right:4px; vertical-align:middle; }}
.b1 {{ border-color:{C['blue']}66; color:{C['blue']}; }}
.b3 {{ border-color:{C['warn']}66; color:{C['warn']}; }}
.pill {{ display:inline-block; padding:3px 10px; border-radius:6px; font-weight:700; font-size:12px; }}
.win {{ background:{C['good']}22; color:#5fd35f; }}
.loss {{ background:{C['bad']}22; color:{C['red']}; }}
.exp {{ background:{C['warn']}22; color:{C['warn']}; }}
.open {{ background:{C['blue']}22; color:{C['blue']}; }}
.bar {{ position:relative; height:10px; background:{C['line']}; border-radius:5px; margin:8px 0 4px; }}
.bar .z {{ position:absolute; left:66.7%; top:-3px; width:2px; height:16px; background:{C['sub']}; }}
.bar .f {{ position:absolute; top:0; height:10px; border-radius:5px; }}
.bar .m {{ position:absolute; top:-3px; width:4px; height:16px; border-radius:2px; background:{C['text']}; }}
.note {{ background:{C['surface']}; border-left:4px solid {C['warn']}; padding:10px 14px; border-radius:6px; color:{C['sub']}; font-size:12px; }}
.todo {{ background:{C['surface']}; border-left:4px solid {C['blue']}; padding:10px 14px; border-radius:6px; color:{C['text']}; font-size:12px; line-height:1.55; }}
.todo b {{ color:{C['text']}; }}
.muted {{ color:{C['muted']}; font-size:12px; }}
.tt {{ width:100%; border-collapse:collapse; margin-top:8px; font-size:12px; }}
.tt th {{ color:{C['muted']}; font-weight:500; font-size:10px; text-align:right; padding:2px 0 4px; }}
.tt th:first-child, .tt td:first-child {{ text-align:left; color:{C['sub']}; width:22%; }}
.tt td {{ text-align:right; padding:5px 0; border-top:1px solid {C['line']}; font-variant-numeric:tabular-nums; }}
.tt td small {{ display:block; color:{C['muted']}; font-size:10px; }}
.tt, .tt tr, .tt th, .tt td {{ border-left:none !important; border-right:none !important; border-bottom:none !important; background:transparent !important; }}
.tt th {{ border-top:none !important; }}
.tfoot {{ display:flex; justify-content:space-between; align-items:baseline; margin-top:8px; padding-top:8px; border-top:1px solid {C['line']}; }}
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


def simulate(trades, cap=1_000_000, slots=10, sizing='split'):
    """100만 원 시뮬레이션. trades: in,out,r(소수)
    sizing='split': 그날 쓸 수 있는 현금을 그날 신호 수로 똑같이 나눠 전부 투자(보유 중인 건 그대로) — 지금 봇 방식
    sizing='pct'  : 한 종목 계좌의 10% · 최대 10종목 — 예전 방식"""
    if trades.empty:
        return pd.DataFrame(), cap, 0.0
    T = trades.sort_values('in'); cash = cap; op = []; curve = []; peak = cap; mdd = 0.0
    for dd in sorted(set(T['in']) | set(T['out'])):
        for p in [p for p in op if p[0] <= dd]:
            cash += p[1]; op.remove(p)
        eq = cash + sum(p[2] for p in op)
        today_t = T[T['in'] == dd]
        if sizing == 'split' and len(today_t) and cash > 0:
            sz = cash / len(today_t)
            for t in today_t.itertuples():
                op.append((t.out, sz * (1 + t.r), sz))
            cash = 0.0
            today_t = today_t.iloc[0:0]
        for t in today_t.itertuples():
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


def sim_of(df, sizing='split'):
    """끝난 거래 → (같은 날 같은 종목 1회) 시뮬 결과와 기본 통계"""
    cl = df[df['result_pct'].notna()] if 'result_pct' in df.columns else df.iloc[0:0]
    uniq = cl.sort_values('strategy').drop_duplicates(['ticker', 'signal_date']) if not cl.empty else cl
    trades = pd.DataFrame()
    if not uniq.empty:
        trades = pd.DataFrame({'in': pd.to_datetime(uniq['entry_date'], errors='coerce'), 'out': pd.to_datetime(uniq['close_date'], errors='coerce'),
                               'r': uniq['result_pct'] / 100}).dropna()
    curve, final, mdd = simulate(trades, sizing=sizing)
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
c1.markdown(f'<div class="ss-h1">Surge Scanner</div><div class="ss-sub">마감 전 탐지 → 마감가 매수 · +5% / −20% / 2일 · 마지막 스캔 {scan_time} {stale}</div>', unsafe_allow_html=True)
if c2.button("새로고침", help="60초 캐시를 비우고 다시 읽어요", use_container_width=True):
    st.cache_data.clear(); st.rerun()

VIEWS = ['탐지', '현황', '기록']
try:
    view = st.segmented_control('화면', VIEWS, default='탐지', selection_mode='single', label_visibility='collapsed', key='view') or '탐지'
except Exception:
    view = st.radio('화면', VIEWS, horizontal=True, label_visibility='collapsed', key='view_radio')

# 공통으로 쓰는 것
def _fx_latest():
    try:
        a = _csv('data/live_account.csv')
        v = _num(a['usdkrw']).dropna() if 'usdkrw' in a.columns else pd.Series(dtype=float)
        return float(v.iloc[-1]) if len(v) else np.nan
    except Exception:
        return np.nan
FX = _fx_latest()
def kw(x):
    """달러 → '4,740원' (한국투자 고시 환율). 환율 없으면 빈칸"""
    try:
        return '' if x is None or np.isnan(x) or np.isnan(FX) else f'{x * FX:,.0f}원'
    except TypeError:
        return ''
def dk(x, small=True):
    """'$3.55 · 4,740원'"""
    w = kw(x)
    return f'${x:,.2f}' + (f' <span class="muted" style="font-size:10px">{w}</span>' if (w and small) else (f' · {w}' if w else ''))
act = open_pos[open_pos['status'].isin(['OPEN', 'PENDING'])] if not open_pos.empty and 'status' in open_pos else pd.DataFrame()
n_open = int((open_pos['status'] == 'OPEN').sum()) if not open_pos.empty and 'status' in open_pos else 0
closed_m = mark_executable(closed_v6) if not closed_v6.empty else closed_v6


# ════════════════════════════════════════════════════════════════════════════
if view == '탐지':
    pre_rows = today[today['scan_type'] == 'preclose'] if (not today.empty and 'scan_type' in today.columns) else pd.DataFrame()
    S10m = f'font-size:10px;color:{C["muted"]}'; S10 = 'font-size:10px'; S10b = 'font-size:10px;font-weight:800'
    NM = _csv('data/names_kr.csv'); nm = dict(zip(NM['symbol'], NM['name_kr'])) if not NM.empty else {}
    UNI = _csv('data/ohlcv/universe.csv'); en = dict(zip(UNI['symbol'], UNI['name'])) if not UNI.empty and 'name' in UNI.columns else {}
    if pre_rows.empty:
        body = f'<div style="{S10m};padding:6px 0">추천 없음</div>'
    else:
        g = 'display:grid;grid-template-columns:40% 28% 32%;align-items:center'
        body = ''
        for tk, x in pre_rows.groupby('ticker'):
            p = _num(pd.Series([x.iloc[0].get('price')])).iloc[0]
            body += (f'<div style="{g};padding:10px 0;border-bottom:1px solid {C["line"]}">'
                     f'<div style="min-width:0"><div style="{S10b};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{nm.get(tk) or en.get(tk, tk)}</div><div style="{S10m}">{tk}</div></div>'
                     f'<div style="text-align:right"><div style="{S10b}">${p:,.2f}</div><div style="{S10m}">{kw(p)}</div></div>'
                     f'<div style="text-align:right;white-space:nowrap"><div style="{S10};color:#5fd35f">${p * 1.05:,.2f} <span style="color:{C["muted"]}">{kw(p * 1.05)}</span></div><div style="{S10};color:{C["red"]}">${p * 0.8:,.2f} <span style="color:{C["muted"]}">{kw(p * 0.8)}</span></div></div></div>')
    st.markdown(f'<div class="card" style="padding:14px 14px 6px"><div style="{S10m};margin-bottom:8px">오늘 추천 ({sig_date})</div>{body}</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
elif view == '현황':
    sub = st.segmented_control('성적 보기', ['실제 매매', '시뮬레이션'], default='실제 매매', selection_mode='single', label_visibility='collapsed', key='perf_sub') or '실제 매매'
    if sub == '실제 매매':
        L = _csv('data/live_trades.csv')
        if L.empty:
            st.markdown('<div class="card muted">아직 기록이 없어요. 장 마감 뒤 갱신 때 체결 내역으로 채워져요.</div>', unsafe_allow_html=True)
        else:
            for c in ('weight_pct', 'buy_px', 'sell_px', 'ret_pct', 'cur_pct', 'qty', 'buy_usd', 'sell_usd', 'pnl_usd', 'value_usd', 'buy_fx', 'sell_fx'):
                L[c] = _num(L[c]) if c in L.columns else np.nan
            done = L[L['status'] == 'closed'].sort_values('sell_date'); hold_l = L[L['status'] == 'holding']
            AC = _csv('data/live_account.csv')
            real = done['pnl_usd'].sum(); unreal = hold_l['pnl_usd'].sum() if not hold_l.empty else 0.0
            if not AC.empty:
                AC['equity_usd'] = _num(AC['equity_usd']); AC = AC.sort_values('date')
                d0 = AC['date'].iloc[0]
                start_eq = AC['equity_usd'].iloc[0] - done[done['sell_date'] <= d0]['pnl_usd'].sum() - (hold_l['pnl_usd'].sum() if False else 0)
                now_eq = AC['equity_usd'].iloc[-1]
                cv = pd.concat([pd.DataFrame({'date': [min(L['order_date'])], 'equity_usd': [start_eq]}), AC[['date', 'equity_usd']]], ignore_index=True)
                cv['date'] = pd.to_datetime(cv['date'])
            else:
                start_eq = now_eq = np.nan; cv = pd.DataFrame()
            chg = (now_eq / start_eq - 1) * 100 if start_eq and not np.isnan(start_eq) else np.nan
            w = (done['result'] == '익절').sum(); l_ = (done['result'] == '손절').sum(); e_ = (done['result'] == '만기').sum()
            usd = lambda x: '—' if x is None or (isinstance(x, float) and np.isnan(x)) else (f'+${x:,.2f}' if x > 0 else (f'−${-x:,.2f}' if x < 0 else '$0.00'))
            fx_now = np.nan
            if not AC.empty and 'usdkrw' in AC.columns:
                _f = _num(AC['usdkrw']).dropna()
                fx_now = float(_f.iloc[-1]) if len(_f) else np.nan
            def won(x, fx=None, sign=False):
                fx = fx if fx is not None and not (isinstance(fx, float) and np.isnan(fx)) else fx_now
                if x is None or np.isnan(x) or np.isnan(fx):
                    return ''
                v = x * fx
                return (('+' if v > 0 else ('−' if v < 0 else '')) if sign else '') + f'{abs(v) if sign else v:,.0f}원'
            # ── 맨 위: 지금 내 계좌 (주식 + 현금) → 시작 금액 대비 손익 ──
            if not AC.empty and not np.isnan(now_eq):
                last = AC.iloc[-1]
                cash_now = float(_num(pd.Series([last.get('cash_usd')])).iloc[0]); stock_now = float(_num(pd.Series([last.get('stock_usd')])).iloc[0])
                diff = now_eq - start_eq
                sgn = lambda v: '+' if v > 0 else ('−' if v < 0 else '')
                S12 = f'font-size:12px;color:{C["muted"]}'          # 설명·원화 (사용자 지정 12px)
                S14 = 'font-size:14px;font-weight:800'                # 큰 금액 (사용자 지정 14px)
                st.markdown(
                    f'<div class="card" style="padding:16px 16px 14px">'
                    f'<div style="{S12}">내 투자 ({last.get("date", "")})</div>'
                    f'<div style="margin-top:4px"><span style="{S14}">${now_eq:,.2f}</span><span style="{S12};margin-left:6px">{won(now_eq)}</span></div>'
                    f'<div style="display:flex;gap:10px;margin-top:10px">'
                    f'<div style="flex:1;border:1px solid {C["line"]};border-radius:10px;padding:8px 10px">'
                    f'<div style="{S12}">투자 중 (주식)</div><div><span style="{S14}">${stock_now:,.2f}</span><span style="{S12};margin-left:6px">{won(stock_now)}</span></div></div>'
                    f'<div style="flex:1;border:1px solid {C["line"]};border-radius:10px;padding:8px 10px">'
                    f'<div style="{S12}">현금</div><div><span style="{S14}">${cash_now:,.2f}</span><span style="{S12};margin-left:6px">{won(cash_now)}</span></div></div></div>'
                    f'<div style="margin-top:12px;padding-top:10px;border-top:1px solid {C["line"]}">'
                    f'<div style="{S12}">원금 ${start_eq:,.2f} ({won(start_eq)})</div>'
                    f'<div><span style="{S14};color:{color_pct(diff)}">{sgn(diff)}${abs(diff):,.2f} ({fmt_pct(chg)})</span>'
                    f'<span style="{S12};margin-left:6px;color:{color_pct(diff)}">{won(diff, sign=True)}</span></div></div></div>',
                    unsafe_allow_html=True)
            else:
                st.markdown('<div class="card muted">계좌 금액은 장 마감 뒤 갱신 때 채워져요.</div>', unsafe_allow_html=True)
            # ── 투자 중인 종목 ──
            if not hold_l.empty:
                NM = _csv('data/names_kr.csv'); nm = dict(zip(NM['symbol'], NM['name_kr'])) if not NM.empty else {}
                UNI = _csv('data/ohlcv/universe.csv'); en = dict(zip(UNI['symbol'], UNI['name'])) if not UNI.empty and 'name' in UNI.columns else {}
                S12m = f'font-size:10px;color:{C["muted"]}'; S12 = 'font-size:10px'; S14 = 'font-size:10px;font-weight:800'   # 투자 중 박스: 전부 10px (사용자 지정)
                money = lambda v, fx_=None: '—' if v is None or np.isnan(v) else f'${v:,.2f}' + (f' ({won(v, fx_)})' if won(v, fx_) else '')
                g = 'display:grid;grid-template-columns:24% 9% 37% 30%;align-items:center;column-gap:0'
                rows_h = ''
                for r in hold_l.sort_values('buy_date').itertuples():
                    name = nm.get(r.symbol) or en.get(r.symbol, r.symbol)
                    tp = round(r.buy_px * 1.05 + 1e-9, 2) if not np.isnan(r.buy_px) else np.nan
                    sl = round(r.buy_px * 0.80, 2) if not np.isnan(r.buy_px) else np.nan
                    bfx = r.buy_fx if not np.isnan(r.buy_fx) else None
                    rows_h += (f'<div style="{g};padding:10px 0;border-bottom:1px solid {C["line"]}">'
                               f'<div style="min-width:0"><div style="{S14};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{name}</div><div style="{S12m}">{r.symbol}</div></div>'
                               f'<div style="{S14};text-align:right">{"" if np.isnan(r.qty) else f"{r.qty:.0f}주"}</div>'
                               f'<div style="text-align:right"><div style="{S14};color:{color_pct(r.pnl_usd)}">{money(r.value_usd)}</div>'
                               f'<div style="{S12};color:{color_pct(r.pnl_usd)}">{usd(r.pnl_usd)} ({won(abs(r.pnl_usd))}) {abs(r.pnl_usd / r.buy_usd * 100) if r.buy_usd else 0:.1f}%</div></div>'
                               f'<div style="text-align:right;white-space:nowrap"><div style="{S12};color:#5fd35f">${tp:,.2f} <span style="color:{C["muted"]}">{kw(tp)}</span></div>'
                               f'<div style="{S12};color:{C["text"]}">${r.buy_px:,.2f} <span style="color:{C["muted"]}">{kw(r.buy_px)}</span></div>'
                               f'<div style="{S12};color:{C["red"]}">${sl:,.2f} <span style="color:{C["muted"]}">{kw(sl)}</span></div></div></div>')
                st.markdown(f'<div class="card" style="padding:14px 14px 6px"><div style="{S12m};margin-bottom:8px">투자 중 ({len(hold_l)}종목)</div>{rows_h}</div>', unsafe_allow_html=True)
            # ── 결과: 전체 · 전략별 익절/손절/만기 ──
            if not done.empty:
                def strat_of(sym, od):
                    f = f'data/signal_{od}.csv'
                    if not os.path.exists(f): return set()
                    x = _csv(f)
                    if 'scan_type' in x.columns: x = x[x['scan_type'] == 'preclose']
                    return set(x[x['ticker'] == sym]['strategy']) if not x.empty else set()
                dd = done.copy(); dd['strats'] = [strat_of(a, b) for a, b in zip(dd['symbol'], dd['order_date'])]
                T10m = f'font-size:10px;color:{C["muted"]}'; T10b = 'font-size:10px;font-weight:800'
                def line(lbl, sub_):
                    w_ = int((sub_['result'] == '익절').sum()); l2 = int((sub_['result'] == '손절').sum()); e2 = int((sub_['result'] == '만기').sum())
                    pnl = sub_['pnl_usd'].sum()
                    return (f'<div style="display:grid;grid-template-columns:34% 42% 24%;align-items:center;padding:8px 0;border-bottom:1px solid {C["line"]}">'
                            f'<div style="{T10b}">{lbl}</div>'
                            f'<div style="font-size:10px;text-align:right"><b style="color:#5fd35f">익절 {w_}</b> · <b style="color:{C["red"]}">손절 {l2}</b> · <b style="color:{C["warn"]}">만기 {e2}</b></div>'
                            f'<div style="{T10b};text-align:right;color:{color_pct(pnl)}">{usd(pnl)}</div></div>')
                rows_r = line(f'전체 {len(dd)}건', dd)
                for sid in ACTIVE_STRATEGIES:
                    sub_ = dd[dd['strats'].map(lambda z: sid in z)]
                    rows_r += line(f'{STRAT_SHORT[sid]} {STRAT_KR[sid]} {len(sub_)}건', sub_)
                st.markdown(f'<div class="card" style="padding:14px 14px 6px"><div style="{T10m};margin-bottom:6px">끝난 거래</div>{rows_r}</div>', unsafe_allow_html=True)
    else:
        ex = closed_m[closed_m['exec_ok']] if not closed_m.empty else closed_m
        allv = closed_m[~closed_m['is_etf']] if not closed_m.empty else closed_m
        S_old = sim_of(ex)                                   # 예전 계산(추적기: 정확히 +5%/−10%, 비용 1%, 전부 체결)
        # 지금 봇 방식 그대로 다시 계산한 결과(data/sim_realistic.csv: LOC 한도·익절 지정가·5분 손절·만기·수수료)로 바꿔 끼움
        SR = _csv('data/sim_realistic.csv'); SRJ = json.load(open('data/sim_realistic.json')) if os.path.exists('data/sim_realistic.json') else {}
        if not SR.empty and not ex.empty:
            SR = SR[SR['status'] == 'closed'].copy()
            SR['ret_pct'] = _num(SR['ret_pct'])
            mp = {(a, b): (r_, d_, e_) for a, b, r_, d_, e_ in zip(SR['ticker'], SR['signal_date'], SR['ret_pct'], SR['date'], SR['exit'])}
            ex = ex[[(a, b) in mp for a, b in zip(ex['ticker'], ex['signal_date'])]].copy()
            ex['result_pct'] = [mp[(a, b)][0] for a, b in zip(ex['ticker'], ex['signal_date'])]
            ex['close_date'] = [mp[(a, b)][1] for a, b in zip(ex['ticker'], ex['signal_date'])]
            ex['result_status'] = [('WIN' if str(mp[(a, b)][2]).startswith('익절') else ('LOSS' if mp[(a, b)][2] == '손절' else 'EXPIRED')) for a, b in zip(ex['ticker'], ex['signal_date'])]
        S = sim_of(ex); A = sim_of(allv); Sp = sim_of(ex, 'pct')
        ret = (S['final'] / 1_000_000 - 1) * 100 if S['n'] else np.nan
        st.markdown(f'<div class="ss-sec">시뮬레이션 <small>지금 봇 방식 그대로 · {ex["signal_date"].min() if not ex.empty else ""}부터</small></div>', unsafe_allow_html=True)
        kpis = [
            ('hero', '100만 원으로 따라 샀다면', f"{S['final']:,.0f}원" if S['n'] else '—',
             f"{fmt_pct(ret)} · {S['n']}건 · 예전 계산(체결·손절 미끄러짐·수수료 빼기 전)이면 {S_old['final']:,.0f}원" if S['n'] else '아직 없음', color_pct(ret)),
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
                          f'<div class="row" style="margin-top:8px"><div><div class="muted">건당 평균</div><div style="font-size:14px;font-weight:700;color:{color_pct(x["result_pct"].mean())}">{fmt_pct(x["result_pct"].mean())}</div></div>'
                          f'<div><div class="muted">도달률</div><div style="font-size:14px;font-weight:700">{(x["result_status"] == "WIN").mean() * 100:.0f}%</div></div>'
                          f'<div><div class="muted">중간값</div><div style="font-size:14px;font-weight:700">{fmt_pct(x["result_pct"].median())}</div></div></div>'
                          f'<div class="chips"><span><b style="color:#5fd35f">익절 {w}</b></span><span><b style="color:{C["red"]}">손절 {l}</b></span><span><b style="color:{C["warn"]}">만기 {e}</b></span><span>하룻밤 실험 <b>{fmt_pct(on.mean() if len(on) else np.nan)}</b></span></div></div>')
            st.markdown(f'<div class="grid">{cards}</div>', unsafe_allow_html=True)
            nj = ex['judge'].value_counts().to_dict() if 'judge' in ex.columns else {}
            st.markdown(f'<div class="muted" style="margin-top:6px">이기면 +5%, 지면 −20%(갭이면 더 크게)라서 도달률 80%가 넘어야 남아요. 판정: 1분봉 {nj.get("minute", 0)}건 · 일봉 {nj.get("daily", 0)}건. '
                        f'하룻밤 실험 = 신호일 종가에 사서 다음날 시가에 팔았다면(기록용).</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
else:
    st.markdown(f'<div class="ss-sec">끝난 거래 <small>{len(closed)}건</small></div>', unsafe_allow_html=True)
    if closed.empty:
        st.markdown('<div class="muted">없음</div>', unsafe_allow_html=True)
    else:
        f1, f2 = st.columns([1, 3])
        _opts = {f"{STRAT_SHORT[s]} {STRAT_KR[s]}": s for s in ACTIVE_STRATEGIES}
        pick = f1.selectbox('전략', ['전체'] + list(_opts), label_visibility='collapsed')
        res = f2.radio('결과', ['전체', '익절', '손절', '만기'], horizontal=True, label_visibility='collapsed')
        x = closed_m if not closed_m.empty else closed.assign(exec_ok=False, is_etf=False)
        if pick != '전체':
            x = x[x['strategy'] == _opts[pick]]
        if res != '전체':
            x = x[x['result_status'] == {'익절': 'WIN', '손절': 'LOSS', '만기': 'EXPIRED'}[res]]
        x = x.sort_values(['close_date', 'signal_date'], ascending=False).head(300)
        cards = ''
        for r in x.itertuples():
            dh = int(r.days_held) if not np.isnan(r.days_held) else '—'
            tag = '' if r.exec_ok else (f'<span class="badge">ETF</span>' if r.is_etf else f'<span class="badge">마감 뒤 신호</span>')
            cards += (f'<div class="card" style="padding:10px 14px"><div class="row"><span><span class="tk" style="font-size:14px">{r.ticker}</span> '
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
- **신호**: 미국장 마감 30분 전(15:30 ET) 가격으로 감시목록만 확인해서 1. 급락반등·2. 과매도 조건에 맞는 종목을 올려요(데이터가 15분 늦게 와서 15:46에 돌아요). ETF·레버리지 상품은 빼요. 마감 뒤 전 종목 스캔도 돌지만 그건 기록·대조용이에요.
- **매수**: 신호일 **마감가**(종가 주문)에 산 것으로 계산해요.
- **매도**: +5% 익절, −20% 손절, 최대 2거래일. 1분봉으로 어느 쪽이 먼저였는지 봐요. 아침부터 손절가 아래/익절가 위로 시작하면 그 **시가**로 계산해요.
- **비용**: 거래마다 왕복 {COST_PCT:.0f}%를 빼요.
- **100만 원 시뮬레이션**: 마감 30분 전에 이미 조건이 맞았던 신호만. 한 종목에 그때 잔고의 10%, 동시에 최대 10종목. 자리가 없으면 그 신호는 건너뛰어요.
- **하룻밤 실험**: 신호일 종가에 사서 다음날 시가에 팔았다면의 값. 기록만 해요.
- 2016~2026 과거 재시험에서 두 전략 모두 같은 날 변동 큰 종목을 무작위로 산 경우와 큰 차이가 없었어요. **종목 추천이 아니라 기록·검증용 화면**이에요.
""")
