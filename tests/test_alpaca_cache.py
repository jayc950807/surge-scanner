#!/usr/bin/env python3
"""Alpaca 데이터 계층 + 일봉 캐시 테스트 (네트워크 없음 — get() 을 가짜로 바꿈)"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd
import pytest

import alpaca_data as ad
import ohlcv_cache as oc
import scanner

NY = ad.NY


def _bar(t, o, h, l, c, v):
    return {'t': t, 'o': o, 'h': h, 'l': l, 'c': c, 'v': v, 'n': 1, 'vw': c}


# ─── alpaca_data ─────────────────────────────────────────────────────────────

class TestAlpacaData:
    def test_end_cap_uses_delay(self, monkeypatch):
        monkeypatch.setattr(ad, 'now_ny', lambda: datetime(2026, 10, 19, 15, 46, tzinfo=NY))
        assert ad.end_cap().startswith('2026-10-19T15:30')
        assert ad.end_cap(date(2026, 10, 16)).startswith('2026-10-16T23:59')      # 지난 날은 그날 끝까지
        assert ad.end_cap(date(2026, 10, 19)).startswith('2026-10-19T15:30')      # 오늘은 지금-16분

    def test_bars_pagination_and_bad_symbol_split(self, monkeypatch):
        calls = []

        def fake_get(url, params=None, retries=8, allow_400=False):
            calls.append(dict(params))
            syms = params['symbols'].split(',')
            if 'BAD' in syms:
                return None                                   # 400: 잘못된 심볼이 섞임
            if params.get('page_token') == 'p2':
                return {'bars': {'AAA': [_bar('2026-10-16T04:00:00Z', 1, 2, 0.5, 1.5, 100)]}, 'next_page_token': None}
            return {'bars': {s: [_bar('2026-10-15T04:00:00Z', 1, 2, 0.5, 1.2, 100)] for s in syms}, 'next_page_token': 'p2' if 'AAA' in syms else None}

        monkeypatch.setattr(ad, 'get', fake_get)
        df = ad.bars(['AAA', 'BAD', 'CCC'], '1Day', '2026-10-15', '2026-10-16', chunk=10)
        assert set(df['symbol']) == {'AAA', 'CCC'}
        assert len(df[df.symbol == 'AAA']) == 2                       # 두 페이지 합침
        assert str(df['t'].dtype).endswith(', UTC]')
        assert any('BAD' in c['symbols'] for c in calls) and any(c['symbols'] == 'BAD' for c in calls)   # 반으로 쪼개 단독 재시도

    def test_history_daily_shape(self, monkeypatch):
        monkeypatch.setattr(ad, 'now_ny', lambda: datetime(2026, 10, 19, 17, 40, tzinfo=NY))
        monkeypatch.setattr(ad, 'get', lambda url, params=None, **k: {'bars': {'X': [
            _bar('2026-10-16T04:00:00Z', 1, 2, 0.5, 1.5, 100), _bar('2026-10-19T04:00:00Z', 1.6, 2.1, 1.4, 2.0, 200)]}})
        h = ad.history_daily('X', '2026-10-16')
        assert list(h.columns) == ['Open', 'High', 'Low', 'Close', 'Volume']
        assert h.index.tz is None and [d.strftime('%Y-%m-%d') for d in h.index] == ['2026-10-16', '2026-10-19']
        assert h['Close'].tolist() == [1.5, 2.0]

    def test_history_minute_regular_session_only(self, monkeypatch):
        ts = ['2026-10-20 09:29', '2026-10-20 09:30', '2026-10-20 15:59', '2026-10-20 16:00', '2026-10-20 16:01']
        bars = [_bar(pd.Timestamp(x, tz='America/New_York').tz_convert('UTC').isoformat(), 1, 1, 1, i + 1, 5) for i, x in enumerate(ts)]
        monkeypatch.setattr(ad, 'get', lambda url, params=None, **k: {'bars': {'X': bars}})
        m = ad.history_minute('X', date(2026, 10, 20), date(2026, 10, 27))
        assert m['Close'].tolist() == [2, 3, 4] and m.index.tz is None and m.index[-1].hour == 16

    def test_missing_keys_raise(self, monkeypatch):
        for k in ('ALPACA_KEY', 'ALPACA_SECRET', 'APCA_API_KEY_ID', 'APCA_API_SECRET_KEY'):
            monkeypatch.delenv(k, raising=False)
        assert ad.have_keys() is False
        with pytest.raises(ad.AlpacaError):
            ad._headers()


# ─── ohlcv_cache ─────────────────────────────────────────────────────────────

def _day(ticker_prices):
    return pd.DataFrame([{'ticker': t, 'open': p, 'high': p * 1.1, 'low': p * 0.9, 'close': p, 'volume': 1000} for t, p in ticker_prices.items()])


class TestCacheFiles:
    def test_write_load_prune(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        for i, d in enumerate(['2026-10-13', '2026-10-14', '2026-10-15', '2026-10-16']):
            oc.write_day(d, _day({'AAA': 10 + i, 'BBB': 5}))
        assert oc.cached_days() == ['2026-10-13', '2026-10-14', '2026-10-15', '2026-10-16'] and oc.last_cached_day() == '2026-10-16'
        df = oc.load(days=2)
        assert sorted(df['date'].unique()) == ['2026-10-15', '2026-10-16'] and set(df['ticker']) == {'AAA', 'BBB'}
        assert oc.load(symbols={'AAA'})['ticker'].unique().tolist() == ['AAA']
        assert oc.prune(keep_days=3) == ['2026-10-13'] and oc.cached_days()[0] == '2026-10-14'

    def test_adjust_splits_reverse(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        oc.write_day('2026-10-14', _day({'AAA': 0.5, 'BBB': 5}))
        oc.write_day('2026-10-15', _day({'AAA': 5.0, 'BBB': 5}))       # AAA 1:10 역분할 (ex 10-15)
        oc.save_splits(pd.DataFrame([{'type': 'reverse_splits', 'symbol': 'AAA', 'ex_date': '2026-10-15', 'old_rate': 10, 'new_rate': 1, 'process_date': '2026-10-15', 'id': 'x1'}]))
        df = oc.load()
        a = df[df.ticker == 'AAA'].sort_values('date')
        assert a['close'].tolist() == [5.0, 5.0] and a['raw_close'].tolist() == [0.5, 5.0]       # ex 전 ×10, 마지막 행은 실제가
        assert a['volume'].tolist() == [100, 1000] and a['high'].iloc[0] == pytest.approx(5.5)
        assert df[df.ticker == 'BBB']['close'].tolist() == [5.0, 5.0]
        df2 = oc.load(asof='2026-10-14')                                 # as-of 가 ex 전이면 보정하지 않음 (야후처럼)
        assert df2[df2.ticker == 'AAA']['close'].tolist() == [0.5, 5.0]

    def test_filter_universe(self):
        a = pd.DataFrame([
            {'symbol': 'AAPL', 'name': 'Apple Inc. Common Stock', 'exchange': 'NASDAQ', 'status': 'active', 'tradable': True},
            {'symbol': 'SMCX', 'name': 'Defiance Daily Target 2X Long SMCI ETF', 'exchange': 'NASDAQ', 'status': 'active', 'tradable': True},
            {'symbol': 'ABCW', 'name': 'ABC Warrants', 'exchange': 'NASDAQ', 'status': 'active', 'tradable': True},
            {'symbol': 'XYZ', 'name': 'XYZ Corp', 'exchange': 'OTC', 'status': 'active', 'tradable': True},
            {'symbol': 'OLD', 'name': 'Old Corp', 'exchange': 'NYSE', 'status': 'inactive', 'tradable': False},
            {'symbol': 'TQQQ', 'name': 'ProShares UltraPro QQQ', 'exchange': 'NASDAQ', 'status': 'active', 'tradable': True},
            {'symbol': 'BRK.A', 'name': 'Berkshire', 'exchange': 'NYSE', 'status': 'active', 'tradable': True},
            {'symbol': 'GOOD', 'name': 'Good Co Class A Ordinary Shares', 'exchange': 'AMEX', 'status': 'active', 'tradable': True},
        ])
        assert oc.filter_universe(a)['symbol'].tolist() == ['AAPL', 'GOOD']


class FakeAlpaca:
    """update() 에 넣는 가짜 모듈: 어떤 날을 요청했는지 기록하고 합성 봉을 돌려준다."""
    def __init__(self, universe, prices, missing_days=()):
        self.universe, self.prices, self.missing = universe, prices, set(missing_days)
        self.calls = []

    def assets(self, status='active'):
        return pd.DataFrame([{'symbol': s, 'name': f'{s} Inc. Common Stock', 'exchange': 'NASDAQ', 'status': 'active', 'tradable': True} for s in self.universe])

    def daily_bars(self, symbols, start_date, end_date, adjustment='raw', chunk=200):
        self.calls.append(('bars', str(start_date), str(end_date), len(symbols)))
        rows = []
        d = pd.to_datetime(start_date).date(); e = pd.to_datetime(end_date).date()
        while d <= e:
            if oc.is_us_trading_day(d) and d.isoformat() not in self.missing:
                for s in symbols:
                    rows.append({'symbol': s, 't': pd.Timestamp(d.isoformat(), tz='America/New_York').tz_convert('UTC'),
                                 'o': self.prices[s], 'h': self.prices[s], 'l': self.prices[s], 'c': self.prices[s], 'v': 100, 'n': 1, 'vw': 1.0, 'date': d.isoformat()})
            d += timedelta(days=1)
        return pd.DataFrame(rows, columns=['symbol', 't', 'o', 'h', 'l', 'c', 'v', 'n', 'vw', 'date'])

    def corporate_actions(self, start, end, types=('forward_split', 'reverse_split')):
        self.calls.append(('ca', str(start), str(end)))
        return pd.DataFrame([{'type': 'reverse_splits', 'symbol': 'AAA', 'ex_date': '2026-10-21', 'old_rate': 10, 'new_rate': 1, 'process_date': '2026-10-21', 'id': 'ca1'}])


class TestCacheUpdate:
    def _uni(self, n=1200):
        import itertools, string
        return [''.join(c) for c in itertools.islice(itertools.product(string.ascii_uppercase, repeat=3), n)]

    def test_incremental_fetches_only_missing_days(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(oc, 'KEEP_DAYS', 5)
        uni = self._uni(); prices = {s: 10.0 for s in uni}
        for d in ['2026-10-13', '2026-10-14', '2026-10-15']:
            oc.write_day(d, _day({s: 10.0 for s in uni}))
        fake = FakeAlpaca(uni, prices)
        s = oc.update('2026-10-19', alpaca=fake, verbose=False)       # 10-16(금)·10-19(월) 두 거래일만 받아야
        assert s['days'] == ['2026-10-16', '2026-10-19'] and s['rows'] == 2 * s['universe'] and 1100 < s['universe'] < 1200   # W 로 끝나는 심볼 제외
        assert [c for c in fake.calls if c[0] == 'bars'] == [('bars', '2026-10-16', '2026-10-19', s['universe'])]
        assert oc.last_cached_day() == '2026-10-19' and os.path.exists(oc.UNIVERSE_PATH)
        assert s['splits_added'] == 1 and oc.load_splits()['symbol'].tolist() == ['AAA']
        assert s['pruned'] == []  # 5일 보관: 13,14,15,16,19 = 5일 → 정리 없음
        s2 = oc.update('2026-10-19', alpaca=fake, verbose=False)      # 이미 최신 → 받을 날 없음
        assert s2['days'] == [] and len([c for c in fake.calls if c[0] == 'bars']) == 1

    def test_skips_day_with_too_few_rows(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        uni = self._uni(); fake = FakeAlpaca(uni, {s: 10.0 for s in uni}, missing_days=['2026-10-19'])
        oc.write_day('2026-10-16', _day({s: 10.0 for s in uni}))
        s = oc.update('2026-10-19', alpaca=fake, verbose=False)
        assert s['days'] == [] and oc.last_cached_day() == '2026-10-16'   # 그날 데이터가 아직 없으면 저장하지 않음

    def test_bootstrap_when_empty(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(oc, 'BOOTSTRAP_DAYS', 3)
        uni = self._uni(); fake = FakeAlpaca(uni, {s: 10.0 for s in uni})
        s = oc.update('2026-10-19', alpaca=fake, verbose=False)
        assert s['days'] == ['2026-10-14', '2026-10-15', '2026-10-16', '2026-10-19']     # 3거래일 전부터 (19일 제외하고 셈)

    def test_prune_after_update(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(oc, 'KEEP_DAYS', 2)
        uni = self._uni(); fake = FakeAlpaca(uni, {s: 10.0 for s in uni})
        for d in ['2026-10-14', '2026-10-15', '2026-10-16']:
            oc.write_day(d, _day({s: 10.0 for s in uni}))
        s = oc.update('2026-10-19', alpaca=fake, verbose=False)
        assert s['pruned'] == ['2026-10-14', '2026-10-15'] and oc.cached_days() == ['2026-10-16', '2026-10-19']


# ─── scanner: 캐시 기반 판정 + 마감 전 1분봉 ───────────────────────────────────

def _frame(closes, last_hl):
    n = len(closes); c = np.array(closes, float)
    df = pd.DataFrame({'Open': c * 1.01, 'High': c * 1.02, 'Low': c * 0.98, 'Close': c, 'Volume': np.full(n, 50000.0), 'raw_close': c},
                      index=pd.bdate_range(end='2026-10-16', periods=n))
    df.iloc[-1, df.columns.get_loc('High')] = last_hl[0]; df.iloc[-1, df.columns.get_loc('Low')] = last_hl[1]
    return df


class TestScanFrames:
    def test_signals_and_watchlist(self, capsys):
        sig = _frame([20.0] * 30 + [19, 18, 17, 16, 15, 14, 12.6], (15.2, 12.4))       # 전략 1·3 둘 다
        flat = _frame([20.0] * 40, (20.4, 19.6))
        down3 = _frame([20.0] * 34 + [19.5, 19.0, 18.5], (18.8, 18.3))                  # 연속하락 3일 → 감시목록만
        s1, s3, wl = scanner.scan_frames({'SIG': sig, 'FLAT': flat, 'DWN': down3}, '13', '2026-10-16', 'afterclose')
        assert [x['ticker'] for x in s1] == ['SIG'] and [x['ticker'] for x in s3] == ['SIG']
        assert s1[0]['date'] == '2026-10-16' and s1[0]['scan_type'] == 'afterclose' and s1[0]['price'] == 12.6
        assert set(wl['ticker']) == {'SIG', 'DWN'} and wl['asof'].iloc[0] == '2026-10-16'

    def test_price_floor_uses_raw_close(self):
        f = _frame([2.0] * 30 + [1.9, 1.8, 1.7, 1.6, 1.4, 1.2, 0.95], (1.2, 0.9))
        f['raw_close'] = 0.5                                                             # 실제 가격이 바닥 아래
        s1, s3, wl = scanner.scan_frames({'PENNY': f}, '13', '2026-10-16', 'afterclose')
        assert s1 == [] and s3 == [] and wl.empty


class TestPrecloseMinutes:
    def _mins(self, monkeypatch, symbol_prices):
        def fake_minute_bars(symbols, start_dt, end_dt=None, adjustment='raw', chunk=100):
            rows = []
            for s in symbols:
                if s not in symbol_prices:
                    continue
                o, h, l, c = symbol_prices[s]
                ts = pd.date_range('2026-10-19 09:30', '2026-10-19 15:30', freq='1min', tz='America/New_York')
                n = len(ts); px = np.linspace(o, c, n)
                hi = px.copy(); hi[n // 2] = h; lo = px.copy(); lo[n // 3] = l
                rows += [{'symbol': s, 't': t.tz_convert('UTC'), 'o': px[i], 'h': hi[i], 'l': lo[i], 'c': px[i], 'v': 10, 'n': 1, 'vw': px[i]} for i, t in enumerate(ts)]
            rows.append({'symbol': 'PRE', 't': pd.Timestamp('2026-10-19 09:00', tz='America/New_York').tz_convert('UTC'), 'o': 1, 'h': 1, 'l': 1, 'c': 1, 'v': 1, 'n': 1, 'vw': 1})   # 프리마켓 봉은 빠져야
            return pd.DataFrame(rows)
        monkeypatch.setattr(scanner.alpaca_data, 'minute_bars', fake_minute_bars)

    def test_minute_bars_today_aggregation(self, monkeypatch):
        self._mins(monkeypatch, {'AAA': (15.0, 15.2, 12.4, 12.57)})
        out = scanner.minute_bars_today(['AAA', 'PRE', 'NONE'], '2026-10-19')
        assert set(out) == {'AAA'}
        bar = scanner.partial_bar(out['AAA'])
        assert bar['Open'] == 15.0 and bar['High'] == 15.2 and bar['Low'] == 12.4 and bar['Close'] == pytest.approx(12.57)
        assert scanner.partial_bar(None) is None

    def test_preclose_scan_end_to_end(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path); os.makedirs('data')
        # 캐시: 30일 횡보 + 6일 연속 하락 (어제까지), 감시목록에 AAA·DDD
        closes = [20.0] * 30 + [19, 18, 17, 16, 15, 14]
        days = [d.strftime('%Y-%m-%d') for d in pd.bdate_range(end='2026-10-16', periods=len(closes))]
        for d, c in zip(days, closes):
            oc.write_day(d, pd.DataFrame([{'ticker': 'AAA', 'open': c * 1.01, 'high': c * 1.02, 'low': c * 0.98, 'close': c, 'volume': 50000},
                                          {'ticker': 'DDD', 'open': 20.2, 'high': 20.4, 'low': 19.8, 'close': 20.0, 'volume': 50000}]))
        pd.DataFrame([{'ticker': 'AAA', 'asof': '2026-10-16'}, {'ticker': 'DDD', 'asof': '2026-10-16'}]).to_csv('data/watchlist.csv', index=False)
        self._mins(monkeypatch, {'AAA': (15.0, 15.2, 12.4, 12.57), 'DDD': (20.0, 20.1, 19.9, 20.0)})
        monkeypatch.setattr(scanner, '_now_et', lambda: datetime(2026, 10, 19, 15, 46, tzinfo=NY))
        monkeypatch.setattr(scanner, 'market_still_open', lambda now: True)
        s1, s3 = scanner.preclose_scan('13')
        assert [x['ticker'] for x in s1] == ['AAA'] and [x['ticker'] for x in s3] == ['AAA']
        assert s1[0]['scan_type'] == 'preclose' and s1[0]['date'] == '2026-10-19' and s1[0]['price_1530'] == pytest.approx(12.57)
        assert '판정 2종목' in capsys.readouterr().out
