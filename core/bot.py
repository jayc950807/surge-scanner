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
    BOT_POS_PCT     종목당 계좌의 % (기본 10)
    BOT_MAX_POS     동시 보유 최대 (기본 10)
    BOT_MAX_NEW     하루 새 매수 최대 (기본 10)
    BOT_START_EQUITY  시작 금액(USD). 계좌가 여기서 BOT_MAX_DD % 이상 줄면 새 매수 중지 (기본 15)
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
MAX_POS = int(env_f('BOT_MAX_POS', 10))
MAX_NEW = int(env_f('BOT_MAX_NEW', 10))
START_EQ = env_f('BOT_START_EQUITY', 0)
MAX_DD = env_f('BOT_MAX_DD', 15) / 100
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
    held = {h['symbol'] for h in hold}
    hold_val = sum(h['qty'] * (h['price'] or h['avg']) for h in hold)
    ex0 = exchange_of(sigs[0][0]); px0 = b.price(sigs[0][0], ex0) or sigs[0][1] or 1.0
    cash = b.cash(sigs[0][0], ex0, px0)
    equity = cash + hold_val
    if START_EQ > 0 and equity < START_EQ * (1 - MAX_DD):
        log(f'  계좌가 시작 대비 −{MAX_DD*100:.0f}% 아래 → 새 매수 중지', f'  계좌 {equity:,.0f} < 시작 {START_EQ:,.0f} × {1-MAX_DD:.2f} → 새 매수 중지'); return
    slots = MAX_POS - (len(held) - len(exiting))
    per = equity * POS_PCT
    log(f'  신호 {len(sigs)}종목 · 빈자리 {max(slots, 0)}', f'  신호 {len(sigs)} · 빈자리 {slots} · 계좌 {equity:,.0f} · 종목당 {per:,.0f} · 현금 {cash:,.0f}')
    n_new = 0
    for sym, px1530 in sigs:
        if n_new >= min(MAX_NEW, slots):
            log(f'  자리 없음 → {sym} 건너뜀'); continue
        if sym in held:
            log(f'  {sym} 이미 보유 → 건너뜀'); continue
        ex = exchange_of(sym)
        px = b.price(sym, ex) or px1530
        if not px:
            log(f'  {sym} 가격 없음 → 건너뜀'); continue
        lim = round_px(px * (1 + LOC_CAP), up=True)
        qty = math.floor(min(per, cash) / lim)
        if qty < 1:
            log(f'  {sym} 살 돈 부족 → 건너뜀'); continue
        try:
            b.order('buy', sym, ex, qty, lim, '34', '신호 종가 매수')
        except KISError as e:
            log(f'  LOC 거부({str(e)[:40]}) → 지정가로')
            try:
                b.order('buy', sym, ex, qty, round_px(px * 1.01, up=True), '00', '신호 매수')
            except KISError as e2:
                log(f'  {sym} 매수 실패: {str(e2)[:60]}'); continue
        cash -= qty * lim; n_new += 1


# ─── 장중: 익절 주문 걸기 + 손절 감시 ─────────────────────────────────────────
def place_take_profits(b):
    hold = b.holdings(); orders = b.open_orders()
    has_sell = {o['symbol'] for o in orders if o['side'] == 'sell'}
    for h in hold:
        if h['symbol'] in has_sell or h['sellable'] <= 0 or h['avg'] <= 0:
            continue
        try:
            b.order('sell', h['symbol'], h['excg'], h['sellable'], round_px(h['avg'] * (1 + TP) + 1e-9), '00', '+5% 익절 주문')
        except KISError as e:
            log(f'  {h["symbol"]} 익절 주문 실패: {str(e)[:60]}')
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
    print('\n결과:', '✓ 연결 정상 (주문은 하지 않았어요)' if ok else '✗ 위 실패 항목 확인')
    if not ok:
        raise SystemExit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['preclose', 'monitor', 'status', 'check'])
    a = ap.parse_args()
    print(f'봇 {a.cmd} · 모드 {MODE}{" · 새 매수 중지" if PAUSE else ""} · ET {now_et():%Y-%m-%d %H:%M}')
    if MODE == 'off':
        print('  BOT_MODE=off → 아무것도 하지 않음'); return
    b = Broker()
    if not b.has_keys:
        print('  KIS 키 없음 → 계좌 조회 없이 연습만')
    try:
        {'preclose': run_preclose, 'monitor': run_monitor, 'status': run_status, 'check': run_check}[a.cmd](b)
    finally:
        flush_notes(a.cmd)


if __name__ == '__main__':
    main()
