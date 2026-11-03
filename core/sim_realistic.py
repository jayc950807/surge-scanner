#!/usr/bin/env python3
"""
지금 봇이 실제로 하는 방식 그대로 과거 신호를 다시 계산 (1분봉, Alpaca) — 앱 '성적 → 시뮬레이션'에 쓰임.

봇 방식 (core/bot.py 와 같게):
  매수  : 신호 날 15:46 ET 가격 ×1.03 한도의 LOC(장마감 지정가). 종가가 한도보다 높으면 안 사짐.
  익절  : 다음 장부터 매일 09:30:30 에 +5% 지정가(센트 버림). 그때 이미 목표 위면 max(목표, 현재가×0.99) 로 바로 매도.
          걸어 둔 지정가는 가격이 목표보다 1센트 이상 위에서 거래돼야 체결로 봄(보수적).
  손절  : 09:31 부터 1분마다(15:40 까지) 현재가가 산 가격 −10% 이하면 그 가격에서 0.5% 더 불리하게 매도
          (실제 봇은 현재가 −3% 지정가 → 보통 현재가 근처에서 체결).
  만기  : 산 뒤 2거래일째 장 마감가(MOC).
  비용  : 한국투자 수수료 0.25% × 2 = 0.5% (+ 위 체결 불리함). 참고로 예전 계산(비용 1%)도 같이 냄.
  계좌  : 그날 쓸 수 있는 현금을 그날 산 신호 수로 나눠 전부 투자, 1주 단위, 보유 중인 건 그대로.

대상: 앱 성적 화면과 같은 '마감 전에 살 수 있었던 신호'(v6 기록, ETF 제외).
결과: data/sim_realistic.csv, 요약은 실행 요약(annotation)에.
"""
import os, sys, math, json
import numpy as np, pandas as pd
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(__file__))
import alpaca_data as ad

FEE = 0.0025; SLIP = 0.005; TP = 0.05; SL = -0.10; LOC_CAP = 0.03; HOLD = 2
STOP_EVERY = int(os.environ.get('STOP_EVERY', '1'))       # 손절 확인 간격(분). 1 = 지금 봇(2026-11-04부터)
SUF = '' if STOP_EVERY == 1 else f'_{STOP_EVERY}m'


def trades():
    cl = pd.read_csv('data/closed_positions.csv', dtype=str, keep_default_na=False)
    bf = pd.read_csv('data/preclose_backfill.csv', dtype=str, keep_default_na=False)
    cl = cl[cl['strategy'].isin(['1', '3']) & (cl['method'] == 'v6_close_entry')]
    st = cl['scan_type']
    ok = st.eq('preclose').values.copy(); etf = np.zeros(len(cl), bool)
    b = bf.drop_duplicates(['ticker', 'signal_date']).set_index(['ticker', 'signal_date'])
    legacy = ~st.isin(['preclose', 'afterclose']).values
    for i, k in enumerate(zip(cl['ticker'], cl['signal_date'])):
        if k in b.index:
            etf[i] = b.at[k, 'etf'] == 'True'
            if legacy[i]:
                ok[i] = b.at[k, 'flag1530'] == 'True'
    x = cl[ok & ~etf].copy()
    x['result_pct'] = pd.to_numeric(x['result_pct'], errors='coerce')
    x = x[x['result_pct'].notna()].sort_values('strategy').drop_duplicates(['ticker', 'signal_date'])
    return x


def floor_c(v): return math.floor(v * 100 + 1e-9) / 100


def sim_one(sym, sd):
    d0 = datetime.strptime(sd, '%Y-%m-%d')
    m = ad.history_minute(sym, d0.replace(hour=9, minute=30), d0 + timedelta(days=8), adjustment='raw')
    if m is None:
        return dict(status='no_data')
    m['day'] = m.index.normalize()
    days = sorted(m['day'].unique())
    if not days or days[0] != pd.Timestamp(sd):
        return dict(status='no_data')
    g0 = m[m['day'] == days[0]]
    hm0 = g0.index.hour * 60 + g0.index.minute
    p_bot = float(g0[hm0 <= 945]['Close'].iloc[-1]) if (hm0 <= 945).any() else float(g0['Close'].iloc[-1])   # 15:45 쯤 봇이 본 가격
    close0 = float(g0['Close'].iloc[-1])
    cap = math.ceil(p_bot * (1 + LOC_CAP) * 100 - 1e-9) / 100
    if close0 > cap + 1e-9:
        return dict(status='unfilled', p_bot=p_bot, entry=close0, cap=cap)
    entry = close0; tgt = floor_c(entry * (1 + TP)); slp = entry * (1 + SL)
    for di, day in enumerate(days[1:HOLD + 1], start=1):
        g = m[m['day'] == day]
        hm = (g.index.hour * 60 + g.index.minute).values
        o, h, l, c = g['Open'].values, g['High'].values, g['Low'].values, g['Close'].values
        tp_on = False
        for j in range(len(g)):
            t = hm[j]
            if t == 570:                                   # 09:30 봉 → 09:30:30 익절 주문
                px = o[j]
                if px >= tgt:
                    lim = max(tgt, floor_c(px * 0.99))
                    return dict(status='closed', exit='익절(시작가)', day=di, entry=entry, exit_px=lim, date=str(day.date()))
                tp_on = True
                if h[j] >= tgt + 0.01:
                    return dict(status='closed', exit='익절', day=di, entry=entry, exit_px=tgt, date=str(day.date()))
                continue
            if tp_on and h[j] >= tgt + 0.01:
                return dict(status='closed', exit='익절', day=di, entry=entry, exit_px=tgt, date=str(day.date()))
            if (575 if STOP_EVERY == 5 else 571) <= t <= 940 and (t - 575) % STOP_EVERY == 0:     # 확인 시각 (직전 봉 종가 = 그때 현재가)
                cur = c[j - 1] if j > 0 else o[j]
                if cur <= slp:
                    return dict(status='closed', exit='손절', day=di, entry=entry, exit_px=cur * (1 - SLIP), date=str(day.date()))
        if di == HOLD:
            return dict(status='closed', exit='만기', day=di, entry=entry, exit_px=float(c[-1]), date=str(day.date()))
    return dict(status='no_data')


def account(rows, cap=1_000_000.0):
    """나눠서 전부 투자 · 1주 단위 · 보유 중인 건 그대로. rows: in(date), out(date), r(소수), px(산 가격)"""
    T = rows.sort_values('in'); cash = cap; op = []; peak = cap; mdd = 0.0; n = 0
    usd = 1350.0                                           # 원 → 달러 (주 수 계산용, 결과는 원으로)
    for d in sorted(set(T['in']) | set(T['out'])):
        for p in [p for p in op if p[0] <= d]:
            cash += p[1]; op.remove(p)
        today = T[T['in'] == d]
        if len(today) and cash > 0:
            per = cash / len(today)
            for t in today.itertuples():
                q = math.floor(per / (t.px * usd))
                if q < 1: continue
                cost = q * t.px * usd; cash -= cost; op.append((t.out, cost * (1 + t.r), cost)); n += 1
        eq = cash + sum(p[2] for p in op); peak = max(peak, eq); mdd = min(mdd, eq / peak - 1)
    return cash + sum(p[1] for p in op), mdd, n


def main():
    x = trades()
    out = []
    for r in x.itertuples():
        try:
            s = sim_one(r.ticker, r.signal_date)
        except Exception as e:
            s = dict(status='error', err=f'{type(e).__name__}: {str(e)[:80]}')
        s.update(ticker=r.ticker, signal_date=r.signal_date, strategy=r.strategy, tracker_pct=r.result_pct, tracker_status=r.result_status)
        if s.get('status') == 'closed':
            s['ret_pct'] = round(((s['exit_px'] * (1 - FEE)) / (s['entry'] * (1 + FEE)) - 1) * 100, 2)
        out.append(s)
    o = pd.DataFrame(out)
    os.makedirs('data', exist_ok=True); o.to_csv(f'data/sim_realistic{SUF}.csv', index=False)
    c = o[o['status'] == 'closed'].copy()
    c['in'] = pd.to_datetime(c['signal_date']); c['out'] = pd.to_datetime(c['date']); c['r'] = c['ret_pct'] / 100; c['px'] = c['entry']
    f_real, mdd_real, n_real = account(c)
    t = x.copy(); t['in'] = pd.to_datetime(t['entry_date']); t['out'] = pd.to_datetime(t['close_date']); t['r'] = t['result_pct'] / 100
    t['px'] = pd.to_numeric(t['entry_price'], errors='coerce')
    f_old, mdd_old, n_old = account(t.dropna(subset=['in', 'out', 'px']))
    vc = o['status'].value_counts().to_dict(); ex = c['exit'].value_counts().to_dict()
    msg = (f"봇 방식 재계산(손절 {STOP_EVERY}분마다): 신호 {len(o)}건 → 산 것 {len(c)} · 안 사짐(LOC 한도) {vc.get('unfilled', 0)} · 데이터 없음 {vc.get('no_data', 0) + vc.get('error', 0)} | "
           f"{' · '.join(f'{k} {v}' for k, v in ex.items())} | 건당 평균 {c['ret_pct'].mean():+.2f}% | "
           f"100만원 → {f_real:,.0f}원 (최대 낙폭 {mdd_real*100:.0f}%) || 예전 계산(추적기 1분봉·비용 1%·전부 체결) → {f_old:,.0f}원 (최대 낙폭 {mdd_old*100:.0f}%)")
    print(msg); print(f'::notice title=sim::{msg}')
    json.dump(dict(final_real=f_real, mdd_real=mdd_real, n_real=n_real, final_old=f_old, mdd_old=mdd_old, n_old=n_old,
                   counts=vc, exits=ex, mean_real=float(c['ret_pct'].mean()), mean_old=float(x['result_pct'].mean()),
                   updated=datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')), open(f'data/sim_realistic{SUF}.json', 'w'), ensure_ascii=False, indent=1)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        import traceback; tb = traceback.format_exc()
        print(tb); print('::error title=sim::' + tb.replace('\n', ' | ')[-900:]); raise
