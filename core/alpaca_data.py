#!/usr/bin/env python3
"""
Alpaca Market Data / Trading API 래퍼 (무료 Basic 플랜 기준)

  키: 환경변수 ALPACA_KEY / ALPACA_SECRET (또는 APCA_API_KEY_ID / APCA_API_SECRET_KEY) 에서만 읽는다.
      GitHub Actions 에서는 repository secrets 로 넣는다. 키 값은 절대 출력·저장하지 않는다.

  무료 플랜 제약 (docs.alpaca.markets → Market Data FAQ):
    - SIP(전 거래소 통합) 데이터는 '최근 15분'을 조회할 수 없다 → end 를 지금-16분으로 자른다 (end_cap).
    - 스냅샷/latest 계열은 SIP 로 못 쓴다 → 오늘 장중 가격은 1분봉(end=지금-16분)으로 받아 직접 합친다.
    - 요청 한도 200회/분 → 요청 사이 0.34초 쉼.

  엔드포인트 (하네스 pipeline/update_data.py·fetch_trades.py 에서 실제로 쓰던 것):
    GET data.alpaca.markets/v2/stocks/bars            일봉·분봉 (멀티심볼, page_token)
    GET data.alpaca.markets/v1/corporate-actions      분할·역분할
    GET paper-api.alpaca.markets/v2/assets            상장 종목
    GET paper-api.alpaca.markets/v2/clock, /calendar  장 운영 시각
"""
import os
import time
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
import requests

NY = ZoneInfo('America/New_York')
DATA_URL = 'https://data.alpaca.markets'
TRADE_URL = 'https://paper-api.alpaca.markets'       # 종이거래(paper) 키 기준. 실거래 키면 api.alpaca.markets 로 자동 전환
TRADE_URL_ALT = 'https://api.alpaca.markets'
SIP_DELAY_MIN = 16          # 무료 플랜: 최근 15분 SIP 조회 불가 → 여유 1분
REQ_SLEEP = 0.34            # 200회/분 한도
BAR_COLS = ['symbol', 't', 'o', 'h', 'l', 'c', 'v', 'n', 'vw']


class AlpacaError(RuntimeError):
    pass


def have_keys():
    return bool(_key_pair()[0] and _key_pair()[1])


def _key_pair():
    k = os.environ.get('ALPACA_KEY') or os.environ.get('APCA_API_KEY_ID') or ''
    s = os.environ.get('ALPACA_SECRET') or os.environ.get('APCA_API_SECRET_KEY') or ''
    return k.strip(), s.strip()


def _headers():
    k, s = _key_pair()
    if not k or not s:
        raise AlpacaError('Alpaca 키가 없음 — GitHub Secrets 에 ALPACA_KEY / ALPACA_SECRET 을 등록하세요')
    return {'APCA-API-KEY-ID': k, 'APCA-API-SECRET-KEY': s}


def get(url, params=None, retries=8, allow_400=False):
    """GET + 재시도. 429/5xx 는 기다렸다 다시, 400 은 allow_400 이면 None(잘못된 심볼 등). 키는 헤더로만.
    거래 API(paper-api) 가 401/403 이면 실거래 호스트(api)로 한 번 바꿔 본다 (키 종류에 따라 호스트가 다름)."""
    global TRADE_URL
    last = ''
    for a in range(retries):
        try:
            r = requests.get(url, headers=_headers(), params=params, timeout=60)
        except requests.RequestException as e:
            last = str(e)[:120]; time.sleep(5 * (a + 1)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            last = f'{r.status_code}'; time.sleep(10 * (a + 1)); continue
        if r.status_code in (401, 403) and url.startswith(TRADE_URL) and TRADE_URL != TRADE_URL_ALT:
            alt = url.replace(TRADE_URL, TRADE_URL_ALT, 1)
            r2 = requests.get(alt, headers=_headers(), params=params, timeout=60)
            if r2.status_code < 400:
                TRADE_URL = TRADE_URL_ALT
                time.sleep(REQ_SLEEP)
                return r2.json()
        if r.status_code == 400 and allow_400:
            return None
        if r.status_code >= 400:
            raise AlpacaError(f'{r.status_code} {url.split("/v")[-1][:40]} {r.text[:160]}')
        time.sleep(REQ_SLEEP)
        return r.json()
    raise AlpacaError(f'요청 실패({last}): {url}')


def now_ny():
    return datetime.now(NY)


def end_cap(when=None):
    """조회 끝 시각(ISO). when 과 '지금-16분' 중 이른 쪽 — 무료 플랜의 최근 15분 SIP 제한."""
    cap = now_ny() - timedelta(minutes=SIP_DELAY_MIN)
    if when is None:
        return cap.isoformat()
    if isinstance(when, date) and not isinstance(when, datetime):
        when = datetime.combine(when, datetime.min.time().replace(hour=23, minute=59), NY)
    if when.tzinfo is None:
        when = when.replace(tzinfo=NY)
    return min(when, cap).isoformat()


def _iso_start(d):
    if isinstance(d, str):
        d = pd.to_datetime(d).date()
    if isinstance(d, datetime):
        return (d if d.tzinfo else d.replace(tzinfo=NY)).isoformat()
    return datetime.combine(d, datetime.min.time(), NY).isoformat()


def bars(symbols, timeframe, start, end, adjustment='raw', feed='sip', chunk=200, limit=10000):
    """멀티심볼 봉 조회 → DataFrame(symbol, t(UTC), o, h, l, c, v, n, vw). 400 이 나면 심볼 묶음을 반으로 쪼개 다시 시도."""
    symbols = [s for s in dict.fromkeys(symbols) if s]
    rows = []
    for i in range(0, len(symbols), chunk):
        rows += _bars_chunk(symbols[i:i + chunk], timeframe, start, end, adjustment, feed, limit)
    df = pd.DataFrame(rows, columns=BAR_COLS)
    if len(df):
        df['t'] = pd.to_datetime(df['t'], utc=True)
    return df


def _bars_chunk(symbols, timeframe, start, end, adjustment, feed, limit):
    p = {'symbols': ','.join(symbols), 'timeframe': timeframe, 'start': start, 'end': end,
         'adjustment': adjustment, 'feed': feed, 'limit': limit}
    rows = []
    while True:
        j = get(f'{DATA_URL}/v2/stocks/bars', p, allow_400=True)
        if j is None:                                  # 잘못된 심볼이 섞임 → 쪼개서
            if len(symbols) == 1:
                logging.warning(f'{symbols[0]}: bars 400')
                return rows
            m = len(symbols) // 2
            return (_bars_chunk(symbols[:m], timeframe, start, end, adjustment, feed, limit) +
                    _bars_chunk(symbols[m:], timeframe, start, end, adjustment, feed, limit))
        for sym, bs in (j.get('bars') or {}).items():
            rows += [(sym, b['t'], b['o'], b['h'], b['l'], b['c'], b['v'], b.get('n'), b.get('vw')) for b in bs]
        if not j.get('next_page_token'):
            return rows
        p['page_token'] = j['next_page_token']


def daily_bars(symbols, start_date, end_date, adjustment='raw', chunk=200):
    """일봉 (뉴욕 날짜 'date' 열 추가). end_date 는 그날 끝까지(무료 플랜 제한에 걸리면 자동으로 지금-16분)."""
    df = bars(symbols, '1Day', _iso_start(start_date), end_cap(pd.to_datetime(end_date).date()), adjustment, chunk=chunk)
    if len(df):
        df['date'] = df['t'].dt.tz_convert(NY).dt.date.astype(str)
    else:
        df['date'] = pd.Series(dtype=str)
    return df


def minute_bars(symbols, start_dt, end_dt=None, adjustment='raw', chunk=100):
    """1분봉 (t 는 UTC → 호출한 쪽에서 tz_convert). end_dt 생략 시 지금-16분."""
    return bars(symbols, '1Min', _iso_start(start_dt), end_cap(end_dt), adjustment, chunk=chunk)


def assets(status='active'):
    """상장 종목 (symbol, name, exchange, status, tradable, fractionable...)."""
    j = get(f'{TRADE_URL}/v2/assets', {'status': status, 'asset_class': 'us_equity'})
    return pd.DataFrame(j or [])


def corporate_actions(start, end, types=('forward_split', 'reverse_split')):
    """분할·역분할 등 기업행위 → DataFrame(type, symbol, ex_date, old_rate, new_rate, process_date, id ...)."""
    rows = []
    s = pd.to_datetime(start).date(); e = pd.to_datetime(end).date()
    while s <= e:
        seg_end = min(s + timedelta(days=89), e)
        p = {'types': ','.join(types), 'start': s.isoformat(), 'end': seg_end.isoformat(), 'limit': 1000}
        while True:
            j = get(f'{DATA_URL}/v1/corporate-actions', p)
            for typ, lst in (j.get('corporate_actions') or {}).items():
                for x in lst:
                    x = dict(x); x['type'] = typ; rows.append(x)
            if not j.get('next_page_token'):
                break
            p['page_token'] = j['next_page_token']
        s = seg_end + timedelta(days=1)
    df = pd.DataFrame(rows)
    for c in ('type', 'symbol', 'ex_date', 'old_rate', 'new_rate', 'process_date', 'id'):
        if c not in df.columns:
            df[c] = pd.Series(dtype=object)
    return df[['type', 'symbol', 'ex_date', 'old_rate', 'new_rate', 'process_date', 'id']]


def clock():
    """{'timestamp','is_open','next_open','next_close'}"""
    return get(f'{TRADE_URL}/v2/clock')


def calendar(start, end):
    """거래일 달력 (date, open, close) — 조기 마감일은 close 가 13:00."""
    j = get(f'{TRADE_URL}/v2/calendar', {'start': str(start), 'end': str(end)})
    return pd.DataFrame(j or [])


# ─── 추적기용: 야후 history() 와 같은 모양의 DataFrame ───────────────────────

def _to_ohlcv(df):
    out = df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'})[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
    out[['Open', 'High', 'Low', 'Close', 'Volume']] = out[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)
    return out


def history_daily(symbol, start_date, end_date=None, adjustment='split'):
    """한 종목 일봉 (분할 보정, 인덱스 = 날짜 naive) — 야후 Ticker.history(auto_adjust=False) 대체."""
    end_date = end_date or now_ny().date()
    df = daily_bars([symbol], start_date, end_date, adjustment=adjustment, chunk=1)
    if df.empty:
        return None
    df = df.sort_values('t')
    out = _to_ohlcv(df)
    out.index = pd.DatetimeIndex(pd.to_datetime(df['date']).values)
    return out


def history_minute(symbol, start_dt, end_dt=None, adjustment='split', regular_only=True):
    """한 종목 1분봉 (인덱스 = 뉴욕 시각 naive). regular_only 면 09:30~16:00 봉만 (16:00 = 종가 체결 봉)."""
    df = minute_bars([symbol], start_dt, end_dt, adjustment=adjustment, chunk=1)
    if df.empty:
        return None
    df = df.sort_values('t')
    out = _to_ohlcv(df)
    out.index = pd.DatetimeIndex(df['t'].dt.tz_convert(NY).dt.tz_localize(None).values)
    if regular_only:
        hm = out.index.hour * 60 + out.index.minute
        out = out[(hm >= 570) & (hm <= 960)]
    return out if len(out) else None
