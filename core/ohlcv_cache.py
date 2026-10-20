#!/usr/bin/env python3
"""
일봉 캐시 — data/ohlcv/
  YYYY-MM-DD.parquet   그날 전 종목 원시(분할 보정 전) 일봉: ticker, open, high, low, close, volume
                       하루 한 파일이라 git 에는 매일 새 파일 하나(~200KB)만 추가된다.
  splits.csv           분할·역분할 (symbol, ex_date, old_rate, new_rate, type, id) — 불러올 때 보정에 쓴다
  universe.csv         상장 종목 (symbol, name, exchange) — 매일 갱신

  갱신(update): 마지막 캐시 날짜 다음 날부터 '끝난 거래일'까지만 받는다 → 평소엔 하루치(요청 30번, 10초쯤).
  캐시가 비어 있으면 KEEP_DAYS 거래일치를 처음 한 번 받는다(1~2분).
  불러오기(load): 원시 일봉에 'as-of 날짜까지 효력이 생긴' 분할만 보정 → 야후 일봉과 같은 기준
                  (마지막 행은 항상 실제 가격). 야후처럼 거래량은 분할 반대 방향으로 보정.
"""
import os
import re
import glob
import logging
from datetime import timedelta

import numpy as np
import pandas as pd

from shared_config import LEVERAGED_ETF, is_us_trading_day, get_last_trading_date

CACHE_DIR = os.path.join('data', 'ohlcv')
SPLITS_PATH = os.path.join(CACHE_DIR, 'splits.csv')
UNIVERSE_PATH = os.path.join(CACHE_DIR, 'universe.csv')
KEEP_DAYS = 160             # 보관 거래일 수 (스캐너는 120일 창을 씀)
BOOTSTRAP_DAYS = 150        # 캐시가 비어 있을 때 처음 받는 거래일 수
EXCHANGES = ('NYSE', 'NASDAQ', 'AMEX')
# 보통주가 아닌 증권(워런트·유닛·라이츠·우선주·ETF 등) 이름 패턴 — 하네스 pipeline/update_data.py 와 동일
NON_COMMON = re.compile(r"\b(?:warrants?|units?|rights?|preferred|depositary shares? representing|notes due|etf|etn|fund|trust units|index|ishares|proshares|direxion|spdr)\b", re.I)
BAR_COLS = ['ticker', 'open', 'high', 'low', 'close', 'volume']


# ─── 파일 ────────────────────────────────────────────────────────────────────

def _day_path(d):
    return os.path.join(CACHE_DIR, f'{d}.parquet')


def cached_days():
    """캐시에 있는 날짜(문자열) 오름차순."""
    out = []
    for p in glob.glob(os.path.join(CACHE_DIR, '????-??-??.parquet')):
        out.append(os.path.basename(p)[:10])
    return sorted(out)


def last_cached_day():
    d = cached_days()
    return d[-1] if d else None


def write_day(d, df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df = df[BAR_COLS].copy()
    df['ticker'] = df['ticker'].astype(str)
    for c in ('open', 'high', 'low', 'close'):
        df[c] = df[c].astype('float64')
    df['volume'] = df['volume'].astype('int64')
    df = df.drop_duplicates('ticker').sort_values('ticker').reset_index(drop=True)
    df.to_parquet(_day_path(d), index=False)
    return len(df)


def prune(keep_days=None):
    keep_days = keep_days or KEEP_DAYS
    days = cached_days()
    old = days[:-keep_days] if len(days) > keep_days else []
    for d in old:
        try:
            os.remove(_day_path(d))
        except OSError:
            pass
    return old


def load_splits():
    if not os.path.exists(SPLITS_PATH):
        return pd.DataFrame(columns=['type', 'symbol', 'ex_date', 'old_rate', 'new_rate', 'process_date', 'id'])
    s = pd.read_csv(SPLITS_PATH, dtype=str, keep_default_na=False)
    s['old_rate'] = pd.to_numeric(s['old_rate'], errors='coerce'); s['new_rate'] = pd.to_numeric(s['new_rate'], errors='coerce')
    return s


def save_splits(df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df = df.copy()
    df = df[df['symbol'].astype(str).str.len() > 0]
    df = df.drop_duplicates('id') if 'id' in df.columns else df.drop_duplicates()
    df.sort_values(['ex_date', 'symbol']).to_csv(SPLITS_PATH, index=False)


def load_universe():
    if not os.path.exists(UNIVERSE_PATH):
        return pd.DataFrame(columns=['symbol', 'name', 'exchange'])
    return pd.read_csv(UNIVERSE_PATH, dtype=str, keep_default_na=False)


def filter_universe(assets_df):
    """Alpaca assets → 운영 대상 종목: NYSE/NASDAQ/AMEX, 거래 가능, 보통주 이름, 알파벳 1~5자, W 로 끝나지 않음, 레버리지 ETF 제외."""
    a = assets_df.copy()
    if a.empty:
        return pd.DataFrame(columns=['symbol', 'name', 'exchange'])
    for c in ('symbol', 'name', 'exchange', 'status', 'tradable'):
        if c not in a.columns:
            a[c] = ''
    a['symbol'] = a['symbol'].astype(str); a['name'] = a['name'].fillna('').astype(str)
    ok = (a['exchange'].isin(EXCHANGES)
          & (a['status'].astype(str) == 'active')
          & (a['tradable'].astype(str).str.lower() != 'false')
          & a['symbol'].str.fullmatch(r'[A-Z]{1,5}')
          & ~a['symbol'].str.endswith('W')
          & ~a['symbol'].isin(LEVERAGED_ETF)
          & ~a['name'].str.contains(NON_COMMON))
    out = a.loc[ok, ['symbol', 'name', 'exchange']].drop_duplicates('symbol').sort_values('symbol').reset_index(drop=True)
    return out


def save_universe(df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df[['symbol', 'name', 'exchange']].to_csv(UNIVERSE_PATH, index=False)


# ─── 갱신 ────────────────────────────────────────────────────────────────────

def trading_days_between(start_excl, end_incl):
    """start 다음 날부터 end 까지의 미국 거래일 목록 (문자열)."""
    out = []
    d = pd.to_datetime(start_excl).date() + timedelta(days=1)
    e = pd.to_datetime(end_incl).date()
    while d <= e:
        if is_us_trading_day(d):
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def bootstrap_start(end_date, n_days=None):
    """end_date 에서 n_days 거래일 전 날짜 (end_date 제외하고 센다)."""
    n_days = n_days or BOOTSTRAP_DAYS
    d = pd.to_datetime(end_date).date(); cnt = 0
    while cnt < n_days:
        d -= timedelta(days=1)
        if is_us_trading_day(d):
            cnt += 1
    return d.isoformat()


def update(through_date, alpaca=None, verbose=True):
    """캐시를 through_date(끝난 거래일)까지 채운다. 반환: {'days': [...], 'rows': n, 'universe': n, 'splits_added': n, 'pruned': [...]}"""
    import alpaca_data as _ad
    ad = alpaca or _ad
    through = pd.to_datetime(through_date).date().isoformat()
    summary = {'days': [], 'rows': 0, 'universe': 0, 'splits_added': 0, 'pruned': []}

    # 1) 종목 목록
    try:
        uni = filter_universe(ad.assets('active'))
        if len(uni) >= 1000:
            save_universe(uni)
        else:
            raise RuntimeError(f'종목이 너무 적음({len(uni)})')
    except Exception as e:
        logging.warning(f'universe refresh failed: {e}')
        uni = load_universe()
    summary['universe'] = len(uni)
    symbols = uni['symbol'].tolist()
    if not symbols:
        raise RuntimeError('종목 목록이 없음 (data/ohlcv/universe.csv)')

    # 2) 빠진 거래일 일봉
    last = last_cached_day()
    if last is None:                       # 처음: BOOTSTRAP_DAYS 거래일치
        start = bootstrap_start(through)
        days = [start] + trading_days_between(start, through)
    else:
        days = trading_days_between(last, through)
    if days:
        if verbose:
            print(f'  캐시 갱신: {days[0]} ~ {days[-1]} ({len(days)}거래일, {len(symbols)}종목)')
        df = ad.daily_bars(symbols, days[0], days[-1], adjustment='raw')
        if df.empty:
            logging.warning('일봉을 하나도 못 받음 (데이터 지연?)')
            df = pd.DataFrame(columns=['symbol', 't', 'o', 'h', 'l', 'c', 'v', 'n', 'vw', 'date'])
        df = df.rename(columns={'symbol': 'ticker', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
        for d in days:
            day_df = df[df['date'] == d]
            if len(day_df) < 500:
                if verbose:
                    print(f'    {d}: {len(day_df)}행 — 너무 적어 저장하지 않음(휴장 또는 데이터 지연)')
                continue
            n = write_day(d, day_df)
            summary['days'].append(d); summary['rows'] += n
            if verbose:
                print(f'    {d}: {n}종목 저장')

    # 3) 분할·역분할 (앞으로 10일치 예정분까지 미리 받아 둔다 — 역분할 당일 마감 전 스캔에서 과거 가격을 맞추기 위해)
    try:
        sp = load_splits()
        first_day = cached_days()[0] if cached_days() else through
        if len(sp):
            s_from = pd.to_datetime(sp['ex_date'], errors='coerce').max().date() - timedelta(days=3)
        else:
            s_from = pd.to_datetime(first_day).date() - timedelta(days=30)
        new = ad.corporate_actions(s_from, pd.to_datetime(through).date() + timedelta(days=10))
        if len(new):
            new = new[new['type'].astype(str).str.contains('split')]
            merged = pd.concat([sp, new], ignore_index=True).drop_duplicates('id')
            save_splits(merged)
            summary['splits_added'] = len(merged) - len(sp)
    except Exception as e:
        logging.warning(f'splits refresh failed: {e}')

    # 4) 오래된 파일 정리
    summary['pruned'] = prune()
    return summary


# ─── 불러오기 ────────────────────────────────────────────────────────────────

def load(days=None, asof=None, adjust=True, symbols=None):
    """캐시 일봉을 하나의 long DataFrame 으로 (ticker, date, open, high, low, close, volume, raw_close).
    days: 마지막 n 거래일만. asof: 이 날짜(포함)까지 효력이 생긴 분할만 보정 (기본: 캐시 마지막 날)."""
    all_days = cached_days()
    if not all_days:
        return pd.DataFrame(columns=BAR_COLS + ['date', 'raw_close'])
    if days:
        all_days = all_days[-days:]
    frames = []
    for d in all_days:
        f = pd.read_parquet(_day_path(d))
        if symbols is not None:
            f = f[f['ticker'].isin(symbols)]
        f['date'] = d
        frames.append(f)
    df = pd.concat(frames, ignore_index=True)
    df['raw_close'] = df['close']
    if adjust:
        df = adjust_splits(df, load_splits(), asof or all_days[-1])
    return df.sort_values(['ticker', 'date']).reset_index(drop=True)


def adjust_splits(df, splits, asof):
    """원시 일봉에 분할 보정: ex_date 전의 가격 × (old/new), 거래량 × (new/old). asof 이후 ex_date 분할은 무시."""
    if df.empty or splits is None or splits.empty:
        return df
    sp = splits.copy()
    sp['ex'] = pd.to_datetime(sp['ex_date'], errors='coerce')
    sp = sp[sp['ex'].notna() & (sp['old_rate'] > 0) & (sp['new_rate'] > 0) & (sp['ex'] <= pd.to_datetime(asof))]
    sp = sp[sp['symbol'].isin(df['ticker'].unique())]
    if sp.empty:
        return df
    df = df.copy()
    dates = pd.to_datetime(df['date'])
    factor = np.ones(len(df))
    for sym, g in sp.groupby('symbol'):
        mask = (df['ticker'] == sym).values
        if not mask.any():
            continue
        for _, r in g.iterrows():
            before = mask & (dates < r['ex']).values
            factor[before] *= float(r['old_rate']) / float(r['new_rate'])
    for c in ('open', 'high', 'low', 'close'):
        df[c] = df[c] * factor
    df['volume'] = (df['volume'] / factor).round().astype('int64')
    return df


def per_ticker(df):
    """long → {ticker: DataFrame(Open, High, Low, Close, Volume, raw_close) 날짜 인덱스} — check_signals 입력 모양."""
    out = {}
    cols = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
    for tk, g in df.groupby('ticker', sort=False):
        f = g.rename(columns=cols)[['Open', 'High', 'Low', 'Close', 'Volume', 'raw_close']].copy()
        f.index = pd.DatetimeIndex(pd.to_datetime(g['date']).values)
        out[tk] = f
    return out
