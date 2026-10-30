#!/usr/bin/env python3
"""
자동 매매 봇 (한국투자증권) — v6 규칙을 실제 주문으로

  python core/bot.py preclose   15:46 ET (마감 전 스캔 직후): 2일째 종목 종가 매도(MOC) → 새 신호 종가 매수(LOC)
  python core/bot.py monitor    09:28 ET ~ 15:40 ET: 장 시작 직후 +5% 지정가 매도 걸기, 5분마다 −10% 확인해 손절
  python core/bot.py status     보유 현황 (종목·보유일·수익률만, 금액 없음)

  규칙 (research/STRATEGY_V6.md · 분봉 재현 시뮬과 같은 방식)
    매수: 신호일 15:46 가격 +3% 한도의 LOC(장마감지정가) → 종가에 체결. LOC 가 거부되면 현재가 +1% 지정가.
    익절: 매일 장 시작 직후 평균단가 +5% 지정가 매도 (당일 주문이라 매일 다시 건다)
    손절: 09:35부터 5분마다 현재가 확인, 평균단가 −10% 이하면 걸어 둔 주문 취소 → 현재가 −3% 지정가 매도 (안 팔리면 −7%)
    만기: 산 뒤 2거래일째 장 마감에 MOC 매도 (거부되면 현재가 −3% 지정가)

  안전장치
    BOT_MODE        off(아무것도 안 함) | dry(주문 없이 기록만, 기본) | live(실제 주문)
    BOT_PAUSE       1 이면 새 매수 중지 (보유 종목 익절·손절·만기는 계속)
    BOT_SIZING      split(기본: 그날 쓸 수 있는 현금 ÷ 그날 살 신호 수, 보유 중인 종목은 그대로) | pct(종목당 계좌의 BOT_POS_PCT %)
    BOT_POS_PCT     pct 방식일 때 종목당 계좌의 % (기본 10)
    BOT_MAX_POS     동시 보유 최대 (기본 10)
    BOT_MAX_NEW     하루 새 매수 최대 (기본 10)
    BOT_START_EQUITY  시작 금액(USD). 계좌가 여기서 BOT_MAX_DD % 이상 줄면 새 매수 중지 (기본 60 — 나눠서 전부 투자 방식이라 2026-10-27 사용자 결정으로 15→60)
    BOT_START_DATE  봇 시작일. 이후 마감 전 신호 실적이 20건 넘었는데 평균이 마이너스면 새 매수 중지
  공개 저장소라 로그에는 금액·수량·계좌 정보를 쓰지 않는다. 자세한 내용은 TELEGRAM_TOKEN/TELEGRAM_CHAT_ID 가 있으면 텔레그램으로.
"""
import os
import sys
import math
import time
import json
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(__file__))
from shared_config import is_us_trading_day, is_us_dst
from kis import KIS, KISError, round_px, EXCG_ORDER

TP, SL, MAX_HOLD = 0.05, -0.10, 2
LOC_CAP = 0.03
POLL_MIN = 5


def env_f(name, default):
    try:
        return float(os.environ.get(name, '') or default)
    except ValueError:
        return float(default)


MODE = (os.environ.get('BOT_MODE', '') or 'dry').strip().lower()
PAUSE = (os.environ.get('BOT_PAUSE', '') or '0').strip() in ('1', 'true', 'yes', 'YES')
POS_PCT = env_f('BOT_POS_PCT', 10) / 100
SIZING = os.environ.get('BOT_SIZING', 'split').strip().lower()   # split: 그날 쓸 수 있는 현금을 그날 신호 수로 똑같이 나눔(사용자 지정 2026-10-27) | pct: 종목당 계좌의 POS_PCT
MAX_POS = int(env_f('BOT_MAX_POS', 10))
MAX_NEW = int(env_f('BOT_MAX_NEW', 10))
START_EQ = env_f('BOT_START_EQUITY', 0)
MAX_DD = env_f('BOT_MAX_DD', 60) / 100
START_DATE = (os.environ.get('BOT_START_DATE', '') or '').strip()
DRY_EQUITY = env_f('BOT_DRY_EQUITY', 10000)


# ─── 시간 ────────────────────────────────────────────────────────────────────
def now_et():
    return datetime.now(timezone(timedelta(hours=-4 if is_us_dst(datetime.now(timezone.utc).date()) else -5)))


def sleep_until(t):
    while True:
        d = (t - now_et()).total_seconds()
        if d <= 0:
            return
        time.sleep(min(d, 30))


def trading_days_after(d0, d1):
    """d0 다음 날부터 d1 까지 거래일 수 (d1 포함)"""
    n, d = 0, d0 + timedelta(days=1)
    while d <= d1:
        n += is_us_trading_day(d)
        d += timedelta(days=1)
    return n


# ─── 알림 ────────────────────────────────────────────────────────────────────
_notes = []


def log(public, private=None):
    """public 은 Actions 로그(공개)에, private 는 텔레그램에만"""
    print(public, flush=True)
    _notes.append(private or public)


def flush_notes(title):
    tok, chat = os.environ.get('TELEGRAM_TOKEN', ''), os.environ.get('TELEGRAM_CHAT_ID', '')
    if not (tok and chat and _notes):
        return
    try:
        requests.post(f'https://api.telegram.org/bot{tok}/sendMessage', timeout=15,
                      data={'chat_id': chat, 'text': f'[{title} · {MODE}]\n' + '\n'.join(_notes)[:3900]})
    except Exception:
        print('  (텔레그램 전송 실패)')


# ─── 데이터 ──────────────────────────────────────────────────────────────────
def exchange_of(symbol):
    try:
        u = pd.read_csv('data/ohlcv/universe.csv', dtype=str, keep_default_na=False)
        ex = u.loc[u['symbol'] == symbol, 'exchange']
        return EXCG_ORDER.get(ex.iloc[0], 'NASD') if len(ex) else 'NASD'
    except Exception:
        return 'NASD'


def todays_signals(today):
    p = f'data/signal_{today}.csv'
    if not os.path.exists(p):
        return []
    s = pd.read_csv(p, dtype=str, keep_default_na=False)
    if 'scan_type' not in s.columns:
        return []
    s = s[s['scan_type'] == 'preclose']
    if s.empty:
        return []
    g = s.groupby('ticker').agg(n=('strategy', 'nunique'), px=('price_1530', 'first')).reset_index()
    g = g.sort_values(['n', 'ticker'], ascending=[False, True])
    return [(r.ticker, float(r.px) if r.px else None) for r in g.itertuples()]


def live_check():
    """봇 시작 뒤 마감 전 신호 실적 — 20건 이상인데 평균 마이너스면 False"""
    if not START_DATE or not os.path.exists('data/closed_positions.csv'):
        return True, ''
    c = pd.read_csv('data/closed_positions.csv', dtype=str, keep_default_na=False)
    if 'scan_type' not in c.columns:
        return True, ''
    c = c[(c['scan_type'] == 'preclose') & (c['signal_date'] >= START_DATE)].drop_duplicates(['ticker', 'signal_date'])
    r = pd.to_numeric(c.get('result_pct', pd.Series(dtype=str)), errors='coerce').dropna()
    if len(r) >= 20 and r.mean() < 0:
        return False, f'실전 실적 {len(r)}건 평균 {r.mean():+.2f}% — 새 매수 중지'
    return True, f'실전 실적 {len(r)}건 평균 {r.mean():+.2f}%' if len(r) else ''


# ─── 실제 매매 기록 (2026-10-27 사용자 결정: 금액·수량도 공개 저장소에 기록. 계좌번호·키는 절대 쓰지 않음) ──────
LIVE_FILE = 'data/live_trades.csv'
LIVE_COLS = ['order_date', 'symbol', 'weight_pct', 'limit_px', 'status', 'buy_date', 'buy_px', 'sell_date', 'sell_px', 'ret_pct', 'result', 'cur_pct', 'updated',
             'qty', 'buy_usd', 'sell_usd', 'pnl_usd', 'value_usd', 'buy_fx', 'sell_fx']
ACCT_FILE = 'data/live_account.csv'   # date, equity_usd, cash_usd, stock_usd, usdkrw (하루 한 줄)
LIVE_START = os.environ.get('BOT_LIVE_START', '2026-10-26')   # 실매매 성적 시작일(미국 날짜) — 첫 실매매 BRAI(10/26 매수, 예전 10% 방식) 포함


def live_load():
    if os.path.exists(LIVE_FILE):
        d = pd.read_csv(LIVE_FILE, dtype=str, keep_default_na=False)
        for c in LIVE_COLS:
            if c not in d.columns:
                d[c] = ''
        return d[LIVE_COLS]
    return pd.DataFrame(columns=LIVE_COLS)


def live_record_order(day, sym, weight_pct, lim):
    d = live_load()
    if ((d['order_date'] == day) & (d['symbol'] == sym)).any():
        return
    row = {c: '' for c in LIVE_COLS}
    row.update(order_date=day, symbol=sym, weight_pct=f'{weight_pct:.1f}', limit_px=f'{lim}', status='ordered', updated=day)
    d = pd.concat([d, pd.DataFrame([row])], ignore_index=True)
    os.makedirs('data', exist_ok=True); d.to_csv(LIVE_FILE, index=False)


def run_record(b):
    """체결 내역·잔고로 실매매 기록 갱신 (조회만, 주문 없음)"""
    d = live_load()
    if d.empty:
        log('  실매매 기록 없음'); return
    today = now_et().date(); ts = today.strftime('%Y-%m-%d')
    start = min(d['order_date']).replace('-', '')
    fills = b.k.fills(start, today.strftime('%Y%m%d')) if b.has_keys else []
    hold = {h['symbol']: h for h in (b.holdings() if b.has_keys else [])}
    fx = ''
    try:                                                    # 원화 표시용 환율(한국투자 고시 환율)
        o2, _ = b.k.present_balance() if b.has_keys else ([], {})
        u = [x for x in o2 if (x.get('crcy_cd') or '') == 'USD']
        if u and float(u[0].get('frst_bltn_exrt') or 0) > 0:
            fx = f"{float(u[0]['frst_bltn_exrt']):.2f}"
    except (KISError, ValueError, AttributeError):
        fx = ''
    eq_now = None
    if b.has_keys and (d['weight_pct'] == '').any():       # 비중이 비어 있는 행(직접 넣은 기록)만 지금 계좌로 추정
        try:
            eq_now = b.cash('SPY', 'AMEX', 500) + sum(h['qty'] * (h['price'] or h['avg']) for h in hold.values())
        except KISError:
            eq_now = None
    for i, r in d.iterrows():
        if r['status'] == 'closed' and r['qty'] != '':
            if fx and r['buy_fx'] == '': d.at[i, 'buy_fx'] = fx          # 예전 기록은 지금 환율로 채움(근사)
            if fx and r['sell_fx'] == '': d.at[i, 'sell_fx'] = fx
            continue
        if r['status'] == 'unfilled':
            continue
        od = r['order_date'].replace('-', '')
        buys = [f for f in fills if f['symbol'] == r['symbol'] and f['side'] == 'buy' and f['date'] >= od]
        if not buys:
            if ts > r['order_date']:
                d.at[i, 'status'] = 'unfilled'; d.at[i, 'updated'] = ts
            continue
        bd = min(f['date'] for f in buys); bq = sum(f['qty'] for f in buys); bpx = sum(f['qty'] * f['price'] for f in buys) / bq
        sells = [f for f in fills if f['symbol'] == r['symbol'] and f['side'] == 'sell' and f['date'] >= bd]
        sq = sum(f['qty'] for f in sells)
        d.at[i, 'buy_date'] = f'{bd[:4]}-{bd[4:6]}-{bd[6:]}'; d.at[i, 'buy_px'] = f'{bpx:.4f}'
        d.at[i, 'qty'] = str(bq); d.at[i, 'buy_usd'] = f'{bq * bpx:.2f}'
        if fx and r['buy_fx'] == '': d.at[i, 'buy_fx'] = fx
        try:
            lim = float(r['limit_px'] or 0); w = float(r['weight_pct'] or 0)
            if lim > 0 and w > 0:
                d.at[i, 'weight_pct'] = f'{w * min(bpx / lim, 1.0):.1f}'      # 실제 체결가로 비중 보정
                d.at[i, 'limit_px'] = ''
        except ValueError:
            pass
        if r['weight_pct'] == '' and eq_now:
            pnl = sum(f['qty'] * f['price'] for f in sells) - bpx * min(sq, bq) if sells else 0.0
            d.at[i, 'weight_pct'] = f'{bq * bpx / max(eq_now - pnl, 1e-9) * 100:.1f}'
        if sq >= bq:
            spx = sum(f['qty'] * f['price'] for f in sells) / sq; sd = max(f['date'] for f in sells)
            ret = (spx / bpx - 1) * 100
            d.at[i, 'sell_date'] = f'{sd[:4]}-{sd[4:6]}-{sd[6:]}'; d.at[i, 'sell_px'] = f'{spx:.4f}'; d.at[i, 'ret_pct'] = f'{ret:.2f}'
            d.at[i, 'result'] = '익절' if ret >= TP * 100 - 0.5 else ('손절' if ret <= SL * 100 + 1 else '만기')
            d.at[i, 'status'] = 'closed'; d.at[i, 'cur_pct'] = ''
            d.at[i, 'sell_usd'] = f'{sq * spx:.2f}'; d.at[i, 'pnl_usd'] = f'{sq * spx - bq * bpx:.2f}'; d.at[i, 'value_usd'] = ''
            if fx and r['sell_fx'] == '': d.at[i, 'sell_fx'] = fx
        else:
            h = hold.get(r['symbol'])
            px = (b.price(r['symbol'], h['excg']) or h['price']) if h else None
            d.at[i, 'status'] = 'holding'; d.at[i, 'cur_pct'] = f'{(px / bpx - 1) * 100:.2f}' if px else ''
            d.at[i, 'value_usd'] = f'{(bq - sq) * px:.2f}' if px else ''; d.at[i, 'pnl_usd'] = f'{(bq - sq) * (px - bpx):.2f}' if px else ''
        d.at[i, 'updated'] = ts
    d.to_csv(LIVE_FILE, index=False)
    # 계좌 금액 스냅샷 (하루 한 줄, 같은 날이면 덮어씀)
    if b.has_keys:
        try:
            # 현금 = 주문가능금액. 외화예수금(frcr_dncl_amt_2)은 결제(T+1) 전 매수·매도가 반영되지 않아
            # 산 날에는 현금과 주식이 이중으로 잡힘(2026-10-28 UPC: 76 + 83 = 159 로 잘못 표시). 주문가능금액은 바로 반영되고 수수료도 빠짐.
            cash = b.cash('SPY', 'AMEX', 500)
            stock = sum(h['qty'] * ((b.price(h['symbol'], h['excg']) or h['price']) or h['avg']) for h in hold.values())
            a = pd.read_csv(ACCT_FILE, dtype=str, keep_default_na=False) if os.path.exists(ACCT_FILE) else pd.DataFrame(columns=['date', 'equity_usd', 'cash_usd', 'stock_usd', 'usdkrw'])
            if 'usdkrw' not in a.columns: a['usdkrw'] = fx
            a = a[a['date'] != ts]
            a = pd.concat([a, pd.DataFrame([{'date': ts, 'equity_usd': f'{cash + stock:.2f}', 'cash_usd': f'{cash:.2f}', 'stock_usd': f'{stock:.2f}', 'usdkrw': fx}])], ignore_index=True)
            a.sort_values('date').to_csv(ACCT_FILE, index=False)
        except KISError as e:
            log(f'  계좌 금액 조회 실패: {str(e)[:60]}')
    n = d['status'].value_counts().to_dict()
    log(f'  실매매 기록 갱신: 끝 {n.get("closed", 0)} · 보유 {n.get("holding", 0)} · 주문 {n.get("ordered", 0)} · 미체결 {n.get("unfilled", 0)}')


# ─── 브로커 래퍼 (dry 면 주문만 막음) ──────────────────────────────────────────
class Broker:
    def __init__(self, kis=None):
        self.k = kis or KIS()
        self.live = MODE == 'live'
        self.has_keys = self.k.ready()
        if self.live and not self.has_keys:
            raise SystemExit('BOT_MODE=live 인데 KIS 키가 없음 (KIS_APP_KEY / KIS_APP_SECRET / KIS_ACCOUNT)')

    def holdings(self):
        return self.k.balance() if self.has_keys else []

    def open_orders(self):
        return self.k.open_orders() if self.has_keys else []

    def price(self, sym, ex):
        return self.k.price(sym, ex) if self.has_keys else None

    def cash(self, sym, ex, px):
        return self.k.buying_power(sym, ex, px) if self.has_keys else DRY_EQUITY

    def order(self, side, sym, ex, qty, px, dvsn, why):
        tag = {'34': 'LOC', '33': 'MOC', '00': '지정가'}.get(dvsn, dvsn)
        if not self.live:
            log(f'  [연습] {why}: {sym} {"매수" if side == "buy" else "매도"} {tag}', f'  [연습] {why}: {sym} {side} {tag} {qty}주 @ {px}')
            return 'DRY'
        odno = self.k.order(side, sym, ex, qty, px, dvsn)
        log(f'  ✓ {why}: {sym} {"매수" if side == "buy" else "매도"} {tag} 주문 접수', f'  ✓ {why}: {sym} {side} {tag} {qty}주 @ {px} (#{odno})')
        return odno

    def cancel_sells(self, sym, orders=None):
        for o in (orders if orders is not None else self.open_orders()):
            if o['symbol'] == sym and o['side'] == 'sell':
                if self.live:
                    self.k.cancel(o['odno'], sym, o['excg'], o['qty_left'])
                log(f'  {"[연습] " if not self.live else ""}{sym} 걸어 둔 매도 취소')

    def entry_dates(self, today):
        """보유 종목의 매수 체결일(현지) — 최근 14일 체결 내역에서 가장 최근 매수"""
        if not self.has_keys:
            return {}
        d0 = (today - timedelta(days=14)).strftime('%Y%m%d')
        out = {}
        for f in self.k.fills(d0, today.strftime('%Y%m%d')):
            if f['side'] == 'buy' and f['date']:
                d = datetime.strptime(f['date'], '%Y%m%d').date()
                out[f['symbol']] = max(out.get(f['symbol'], d), d)
        return out


# ─── 마감 전: 만기 매도 + 새 매수 ─────────────────────────────────────────────
def run_preclose(b, now=None):
    now = now or now_et(); today = now.date(); ts = today.strftime('%Y-%m-%d')
    if not is_us_trading_day(today):
        log('  오늘은 미국 휴장일 → 종료'); return
    hm = now.hour * 60 + now.minute
    if not (15 * 60 + 40 <= hm <= 15 * 60 + 49):
        log(f'  ET {now:%H:%M} — 15:40~15:49 밖이라 주문하지 않음'); return
    try:                                                  # 조기 마감일(13:00) 등 장이 이미 닫혔으면 아무것도 안 함
        import alpaca_data
        if alpaca_data.have_keys() and alpaca_data.clock().get('is_open') is False:
            log('  장이 이미 닫혀 있음(조기 마감일 등) → 주문하지 않음'); return
    except Exception:
        pass
    hold = b.holdings(); orders = b.open_orders(); entries = b.entry_dates(today)

    # 1) 2거래일째 종목 → 종가 매도
    exiting = set()
    for h in hold:
        d0 = entries.get(h['symbol'])
        if d0 is None or trading_days_after(d0, today) < MAX_HOLD or h['sellable'] <= 0:
            continue
        exiting.add(h['symbol'])
        b.cancel_sells(h['symbol'], orders); time.sleep(1 if b.live else 0)
        try:
            b.order('sell', h['symbol'], h['excg'], h['sellable'], 0, '33', '2일 만기')
        except KISError as e:
            px = b.price(h['symbol'], h['excg']) or h['price']
            log(f'  MOC 거부({str(e)[:40]}) → 지정가로')
            b.order('sell', h['symbol'], h['excg'], h['sellable'], round_px(px * 0.97), '00', '2일 만기')

    # 2) 새 신호 매수
    sigs = todays_signals(ts)
    if not sigs:
        log('  오늘 마감 전 신호 없음 → 매수 없음'); return
    if PAUSE:
        log('  BOT_PAUSE=1 → 새 매수 중지'); return
    ok, msg = live_check()
    if msg:
        log('  ' + msg)
    if not ok:
        return
    held = {h['symbol'] for h in hold} | {o['symbol'] for o in orders if o['side'] == 'buy'}   # 이미 낸 매수 주문(아직 미체결)도 보유로 침 — 두 번 사지 않게
    hold_val = sum(h['qty'] * (h['price'] or h['avg']) for h in hold)
    ex0 = exchange_of(sigs[0][0]); px0 = b.price(sigs[0][0], ex0) or sigs[0][1] or 1.0
    cash = b.cash(sigs[0][0], ex0, px0)
    equity = cash + hold_val
    if START_EQ > 0 and equity < START_EQ * (1 - MAX_DD):
        log(f'  계좌가 시작 대비 −{MAX_DD*100:.0f}% 아래 → 새 매수 중지', f'  계좌 {equity:,.0f} < 시작 {START_EQ:,.0f} × {1-MAX_DD:.2f} → 새 매수 중지'); return
    slots = MAX_POS - (len(held) - len(exiting))
    # 살 후보: 이미 보유·주문 중이 아닌 신호, 빈자리·하루 한도 안에서
    cands = []
    for sym, px1530 in sigs:
        if sym in held:
            log(f'  {sym} 이미 보유 → 건너뜀'); continue
        if len(cands) >= max(0, min(MAX_NEW, slots)):
            log(f'  자리 없음 → {sym} 건너뜀'); continue
        ex = exchange_of(sym)
        px = b.price(sym, ex) or px1530
        if not px:
            log(f'  {sym} 가격 없음 → 건너뜀'); continue
        cands.append([sym, ex, px, round_px(px * (1 + LOC_CAP), up=True)])
    if SIZING == 'split':
        # 그날 쓸 수 있는 현금을 살 종목 수로 똑같이 나눔. 1주도 못 사는 종목(비싼 것부터)은 빼고 다시 나눔
        while cands and min(cash / len(cands), cash) < max(c[3] for c in cands):
            drop = max(cands, key=lambda c: c[3]); cands.remove(drop)
            log(f'  {drop[0]} 나눈 금액으로 1주도 못 삼 → 건너뜀')
        per = cash / len(cands) if cands else 0.0
        log(f'  신호 {len(sigs)}종목 · 살 종목 {len(cands)} · 현금을 {len(cands)}등분', f'  현금 {cash:,.2f} ÷ {len(cands)} = 종목당 {per:,.2f} (계좌 {equity:,.2f})')
    else:
        per = equity * POS_PCT
        log(f'  신호 {len(sigs)}종목 · 빈자리 {max(slots, 0)}', f'  신호 {len(sigs)} · 빈자리 {slots} · 계좌 {equity:,.0f} · 종목당 {per:,.0f} · 현금 {cash:,.0f}')
    for sym, ex, px, lim in cands:
        qty = math.floor(min(per, cash) / lim)
        if qty < 1:
            log(f'  {sym} 살 돈 부족 → 건너뜀'); continue
        used = lim
        try:
            b.order('buy', sym, ex, qty, lim, '34', '신호 종가 매수')
        except KISError as e:
            log(f'  LOC 거부({str(e)[:40]}) → 지정가로')
            try:
                used = round_px(px * 1.01, up=True)
                b.order('buy', sym, ex, qty, used, '00', '신호 매수')
            except KISError as e2:
                log(f'  {sym} 매수 실패: {str(e2)[:60]}'); continue
        if b.live and equity > 0:
            live_record_order(ts, sym, qty * used / equity * 100, used)
        cash -= qty * lim


# ─── 장중: 익절 주문 걸기 + 손절 감시 ─────────────────────────────────────────
def place_take_profits(b):
    """+5% 익절 지정가. 이미 목표보다 높게 거래 중이면(갭 상승) 현재가 −1% 로 바로 팔리게 건다
    (현재가와 너무 먼 지정가는 증권사가 거부할 수 있음 — 2026-10-27 BRAI 사례). 감시 루프에서 5분마다 다시 불러 빠진 주문을 채운다."""
    hold = b.holdings(); orders = b.open_orders()
    has_sell = {o['symbol'] for o in orders if o['side'] == 'sell'}
    gh = os.environ.get('GITHUB_ACTIONS')
    for h in hold:
        if h['symbol'] in has_sell or h['avg'] <= 0:
            continue
        if h['sellable'] <= 0:
            if gh: print(f'::notice title=take-profit::{h["symbol"]} 팔 수 있는 수량 0 → 익절 주문 못 검(다음 확인 때 다시)')
            continue
        tgt = round_px(h['avg'] * (1 + TP) + 1e-9)
        px = b.price(h['symbol'], h['excg'])
        lim, why = tgt, '+5% 익절 주문'
        if px and px >= tgt:
            lim, why = round_px(px * 0.99), f'목표 이미 넘음({px / h["avg"] * 100 - 100:+.1f}%) → 현재가 −1% 매도'
        try:
            b.order('sell', h['symbol'], h['excg'], h['sellable'], lim, '00', why)
            if gh: print(f'::notice title=take-profit::{h["symbol"]} 익절 매도 주문 접수 — {why}')
        except KISError as e:
            msg = f'  {h["symbol"]} 익절 주문 실패: {str(e)[:80]}'
            log(msg)
            if gh:
                print(f'::warning title=take-profit::{msg.strip()}')
    return hold


def check_stops(b):
    """−10% 이하면 걸어 둔 매도 취소 → 현재가 −3% 지정가, 20초 뒤 남아 있으면 −7%"""
    hold = b.holdings()
    for h in hold:
        if h['avg'] <= 0:
            continue
        px = b.price(h['symbol'], h['excg']) or h['price']
        if not px or px > h['avg'] * (1 + SL):
            continue
        log(f'  ▼ {h["symbol"]} {px / h["avg"] * 100 - 100:+.1f}% → 손절', f'  ▼ {h["symbol"]} 현재 {px} / 평균 {h["avg"]} → 손절')
        b.cancel_sells(h['symbol']); time.sleep(1 if b.live else 0)
        qty = h['qty']
        for disc in (0.97, 0.93):
            try:
                b.order('sell', h['symbol'], h['excg'], qty, round_px(px * disc), '00', '−10% 손절')
            except KISError as e:
                log(f'  손절 주문 실패: {str(e)[:60]}'); break
            if not b.live:
                break
            time.sleep(20)
            left = next((x['qty'] for x in b.holdings() if x['symbol'] == h['symbol']), 0)
            if left <= 0:
                break
            b.cancel_sells(h['symbol']); time.sleep(1)
            qty = left; px = b.price(h['symbol'], h['excg']) or px
    return hold


def redispatch():
    """GitHub Actions 한 작업은 6시간까지라, 다 못 했으면 같은 워크플로를 다시 띄운다"""
    repo, tok = os.environ.get('GITHUB_REPOSITORY'), os.environ.get('GITHUB_TOKEN')
    if not (repo and tok):
        return
    try:
        r = requests.post(f'https://api.github.com/repos/{repo}/actions/workflows/bot_monitor.yml/dispatches', timeout=20,
                          headers={'Authorization': f'Bearer {tok}', 'Accept': 'application/vnd.github+json'}, json={'ref': 'main'})
        print('  다음 감시 작업 예약:', r.status_code)
    except Exception as e:
        print('  다음 감시 작업 예약 실패:', type(e).__name__)


def run_monitor(b, max_minutes=350):
    start = time.time(); n0 = now_et(); today = n0.date()
    if not is_us_trading_day(today):
        log('  오늘은 미국 휴장일 → 종료'); return
    end = n0.replace(hour=15, minute=40, second=0, microsecond=0)
    if n0 >= end:
        log('  15:40 ET 지남 → 종료'); return
    open_t = n0.replace(hour=9, minute=30, second=30, microsecond=0)
    sleep_until(open_t)
    log(f'  감시 시작 ET {now_et():%H:%M}')
    place_take_profits(b)
    nxt = max(now_et(), n0.replace(hour=9, minute=35, second=5, microsecond=0))
    while True:
        sleep_until(nxt)
        if now_et() >= end:
            log('  15:40 ET — 감시 끝 (만기 매도는 마감 전 작업이 처리)'); return
        try:
            check_stops(b)
            place_take_profits(b)      # 빠진 익절 주문(거부·실패·갭 상승) 다시 걸기 — 이미 걸려 있으면 건너뜀
        except KISError as e:
            print('  확인 실패:', str(e)[:80])
        if (time.time() - start) / 60 > max_minutes:
            log('  작업 시간 한도 → 다음 작업으로 넘김'); redispatch(); return
        nxt = nxt + timedelta(minutes=POLL_MIN)


def run_status(b):
    today = now_et().date(); hold = b.holdings(); entries = b.entry_dates(today)
    if not hold:
        log('  보유 없음'); return
    for h in hold:
        d0 = entries.get(h['symbol']); n = trading_days_after(d0, today) if d0 else '?'
        r = (h['price'] / h['avg'] - 1) * 100 if h['avg'] else 0
        log(f'  {h["symbol"]} {n}/{MAX_HOLD}일 {r:+.1f}%', f'  {h["symbol"]} {h["qty"]}주 평균 {h["avg"]} 현재 {h["price"]} {n}/{MAX_HOLD}일 {r:+.1f}%')


def run_check(b):
    """연결 확인 — 조회만 하고 주문은 하지 않는다. 금액은 출력하지 않는다."""
    k = b.k
    if not b.has_keys:
        print('  ✗ 키가 비어 있음 — Secrets 이름(KIS_APP_KEY / KIS_APP_SECRET / KIS_ACCOUNT)과 계좌번호 10자리 확인'); raise SystemExit(1)
    ok = True
    def step(name, fn):
        nonlocal ok
        try:
            msg = f'✓ {name}: {fn()}'
        except Exception as e:
            ok = False; msg = f'✗ {name}: {str(e)[:120]}'
        print('  ' + msg)
        if os.environ.get('GITHUB_ACTIONS'):
            print(f'::notice title=check::{msg}')        # 결과를 실행 요약(annotation)에도 남김 — 금액 없음
    step('토큰 발급(앱키·시크릿)', lambda: '성공' if k.auth() else '실패')
    step('미국 주식 현재가(SPY)', lambda: '받음' if k.price('SPY', 'AMEX') else '값 없음 — 해외 시세 신청 확인')
    step('잔고 조회(계좌번호)', lambda: f'보유 {len(k.balance())}종목')
    step('주문가능금액', lambda: '있음' if k.buying_power('SPY', 'AMEX', 500) > 0 else '0 — 입금·통합증거금·환전 확인')
    step('미체결 조회', lambda: f'{len(k.open_orders())}건')
    def detail():
        oo = k.open_orders(); out = []
        for h in k.balance():
            px = k.price(h['symbol'], h['excg']) or h['price']
            pct = f'{(px / h["avg"] - 1) * 100:+.1f}%' if h['avg'] and px else '?'
            sells = sum(1 for o in oo if o['symbol'] == h['symbol'] and o['side'] == 'sell')
            out.append(f'{h["symbol"]} 평균 대비 {pct} · 팔 수 있음 {"예" if h["sellable"] > 0 else "아니오"} · 걸린 매도 {sells}건')
        return ' / '.join(out) or '없음'
    step('보유 상세(금액·수량 없음)', detail)
    def cash_detail():
        o2, o3 = k.present_balance()
        usd = [x for x in o2 if (x.get('crcy_cd') or '') == 'USD']
        keys2 = ('crcy_cd', 'frcr_dncl_amt_2', 'frcr_drwg_psbl_amt_1', 'frst_bltn_exrt', 'frcr_evlu_amt2')
        keys3 = ('tot_asst_amt', 'tot_dncl_amt', 'wdrw_psbl_tot_amt', 'frcr_evlu_tota', 'evlu_amt_smtl_amt', 'tot_frcr_cblc_smtl')
        a = ' | '.join(', '.join(f'{k_}={x.get(k_)}' for k_ in keys2 if k_ in x) for x in (usd or o2[:2]))
        b_ = ', '.join(f'{k_}={o3.get(k_)}' for k_ in keys3 if k_ in o3)
        return f'USD: {a or "없음"} / 합계: {b_ or "없음"} / 주문가능(USD)={k.buying_power("SPY", "AMEX", 500)}'
    step('현금 상세(금액 공개 — 사용자 결정)', cash_detail)
    def today_fills():
        d = now_et().strftime('%Y%m%d'); y = (now_et() - timedelta(days=4)).strftime('%Y%m%d')
        f = k.fills(y, d)
        return ', '.join(f'{x["date"]} {x["symbol"]} {"매수" if x["side"] == "buy" else "매도"}' for x in f) or '없음'
    step('최근 체결(최근 4일, 금액·수량 없음)', today_fills)
    print('\n결과:', '✓ 연결 정상 (주문은 하지 않았어요)' if ok else '✗ 위 실패 항목 확인')
    if not ok:
        raise SystemExit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['preclose', 'monitor', 'status', 'check', 'record'])
    a = ap.parse_args()
    print(f'봇 {a.cmd} · 모드 {MODE}{" · 새 매수 중지" if PAUSE else ""} · ET {now_et():%Y-%m-%d %H:%M}')
    if MODE == 'off':
        print('  BOT_MODE=off → 아무것도 하지 않음'); return
    b = Broker()
    if not b.has_keys:
        print('  KIS 키 없음 → 계좌 조회 없이 연습만')
    try:
        {'preclose': run_preclose, 'monitor': run_monitor, 'status': run_status, 'check': run_check, 'record': run_record}[a.cmd](b)
    finally:
        flush_notes(a.cmd)


if __name__ == '__main__':
    main()
