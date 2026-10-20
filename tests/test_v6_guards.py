#!/usr/bin/env python3
"""v6 운영 안전장치 테스트 — 장중 실행 차단, 마감 전 신호 우선, 조기 마감, 16:00 봉, 재계산 표시"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
from datetime import datetime, timedelta, timezone
import pandas as pd
import pytest

import scanner
import tracker

ET_SUMMER = timezone(timedelta(hours=-4))


def et(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=ET_SUMMER)


class Boom(Exception):
    pass


def boom(*a, **k):
    raise Boom('should not be called')


# ─── 같은 날 신호 중복: 마감 전(preclose) 신호가 이긴다 ───────────────────────
class TestDedupePreferPreclose:
    def _rows(self, first, second):
        return pd.DataFrame([
            {'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-19', 'price': 12.58, 'scan_type': first},
            {'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-19', 'price': 12.57, 'scan_type': second},
            {'strategy': '3', 'ticker': 'BBB', 'date': '2026-10-19', 'price': 5.0, 'scan_type': second},
        ])

    def test_afterclose_saved_first_still_loses(self):
        out = scanner.dedupe_prefer_preclose(self._rows('afterclose', 'preclose'))
        a = out[(out.ticker == 'AAA')]
        assert len(a) == 1 and a['scan_type'].iloc[0] == 'preclose' and a['price'].iloc[0] == 12.57
        assert len(out) == 2

    def test_preclose_saved_first_wins(self):
        out = scanner.dedupe_prefer_preclose(self._rows('preclose', 'afterclose'))
        a = out[(out.ticker == 'AAA')]
        assert len(a) == 1 and a['scan_type'].iloc[0] == 'preclose' and a['price'].iloc[0] == 12.58

    def test_order_of_other_rows_is_kept(self):
        out = scanner.dedupe_prefer_preclose(self._rows('afterclose', 'preclose'))
        assert out['ticker'].tolist() == ['AAA', 'BBB']

    def test_without_scan_type_column(self):
        df = pd.DataFrame([{'strategy': 1, 'ticker': 'AAA', 'date': 'd', 'price': 1.0}, {'strategy': '1', 'ticker': 'AAA', 'date': 'd', 'price': 2.0}])
        out = scanner.dedupe_prefer_preclose(df)
        assert len(out) == 1 and out['price'].iloc[0] == 1.0


# ─── 마감 뒤 전 종목 스캔: 언제 돌려도 '마지막 확정 거래일'까지만 다룬다 ───────
class TestAfterCloseAnytime:
    def _run(self, monkeypatch, now, argv):
        seen = {}
        monkeypatch.setattr(scanner, '_now_et', lambda: now)
        monkeypatch.setattr(scanner.alpaca_data, 'have_keys', lambda: True)
        monkeypatch.setattr(scanner, 'already_scanned', lambda through: False)
        def fake_update(through, retries=3, wait_sec=120):
            seen['through'] = through; raise Boom()
        monkeypatch.setattr(scanner, 'update_cache', fake_update)
        monkeypatch.setattr(sys, 'argv', argv)
        with pytest.raises(Boom):
            scanner.main()
        return seen['through']

    def test_during_session_uses_yesterday(self, monkeypatch):
        assert self._run(monkeypatch, et(2026, 10, 20, 9, 42), ['scanner.py']) == '2026-10-19'     # 화요일 장중 → 월요일까지

    def test_premarket_uses_yesterday(self, monkeypatch):
        assert self._run(monkeypatch, et(2026, 10, 19, 7, 0), ['scanner.py']) == '2026-10-16'      # 월요일 아침 → 금요일

    def test_right_after_close_waits_for_settle(self, monkeypatch):
        assert self._run(monkeypatch, et(2026, 10, 19, 16, 7), ['scanner.py']) == '2026-10-16'     # 16:07 → 아직 어제

    def test_after_settle_uses_today(self, monkeypatch):
        assert self._run(monkeypatch, et(2026, 10, 19, 16, 25), ['scanner.py']) == '2026-10-19'

    def test_weekend_uses_friday(self, monkeypatch):
        assert self._run(monkeypatch, et(2026, 10, 18, 10, 0), ['scanner.py']) == '2026-10-16'

    def test_no_keys_exits_before_network(self, monkeypatch, capsys):
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 17, 40))
        monkeypatch.setattr(scanner, 'update_cache', boom)
        monkeypatch.setattr(scanner.alpaca_data, 'have_keys', lambda: False)
        monkeypatch.setattr(sys, 'argv', ['scanner.py'])
        with pytest.raises(SystemExit):
            scanner.main()
        assert 'ALPACA_KEY' in capsys.readouterr().out


# ─── 마감 전 스캔 안전장치 ────────────────────────────────────────────────────
class TestPrecloseGuards:
    @pytest.fixture
    def wl(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        os.makedirs('data')
        pd.DataFrame([{'ticker': 'AAA', 'asof': '2026-10-16'}]).to_csv('data/watchlist.csv', index=False)
        monkeypatch.setattr(scanner, 'minute_bars_today', boom)          # 여기까지 오면 '다운로드 시작'
        monkeypatch.setattr(scanner, 'market_still_open', lambda now: True)
        return tmp_path

    def test_too_early(self, wl, monkeypatch, capsys):
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 15, 39))
        assert scanner.preclose_scan('13') == ([], [])
        assert '15:40 ET 전' in capsys.readouterr().out

    def test_too_late(self, wl, monkeypatch):
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 15, 49))
        assert scanner.preclose_scan('13') == ([], [])

    def test_in_window_starts_download(self, wl, monkeypatch):
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 15, 46))
        with pytest.raises(Boom):
            scanner.preclose_scan('13')

    def test_early_close_day_skips(self, wl, monkeypatch, capsys):
        # 추수감사절 다음 날(13:00 조기 마감): 15:30 에 SPY 마지막 봉이 오래됐으면 생략
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 11, 27, 15, 46))
        monkeypatch.setattr(scanner, 'market_still_open', lambda now: False)
        assert scanner.preclose_scan('13') == ([], [])
        assert '이미 끝남' in capsys.readouterr().out

    def test_holiday_skips(self, wl, monkeypatch):
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 11, 26, 15, 30))   # Thanksgiving
        assert scanner.preclose_scan('13') == ([], [])

    def test_already_ran_today_skips(self, wl, monkeypatch):
        pd.DataFrame([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-19', 'scan_type': 'preclose'}]).to_csv('data/signal_2026-10-19.csv', index=False)
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 15, 36))
        assert scanner.preclose_scan('13') == ([], [])

    def test_main_preclose_on_holiday_writes_no_file(self, wl, monkeypatch):
        monkeypatch.setattr(scanner.alpaca_data, 'have_keys', lambda: True)
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 11, 26, 15, 30))
        monkeypatch.setattr(sys, 'argv', ['scanner.py', '--preclose'])
        scanner.main()
        assert not os.path.exists('data/signal_2026-11-26.csv')


class TestMarketStillOpen:
    def test_clock_open(self, monkeypatch):
        monkeypatch.setattr(scanner.alpaca_data, 'clock', lambda: {'is_open': True, 'next_close': '2026-10-19T16:00:00-04:00'})
        assert scanner.market_still_open(et(2026, 10, 19, 15, 46)) is True

    def test_clock_closed_on_early_close_day(self, monkeypatch):
        monkeypatch.setattr(scanner.alpaca_data, 'clock', lambda: {'is_open': False, 'next_open': '2026-11-30T09:30:00-05:00'})
        assert scanner.market_still_open(et(2026, 11, 27, 15, 46)) is False

    def test_failure_is_unknown(self, monkeypatch):
        monkeypatch.setattr(scanner.alpaca_data, 'clock', boom)
        assert scanner.market_still_open(et(2026, 10, 19, 15, 46)) is None


# ─── 1분봉: 진입일에 고정한 7일 창, 16:00 종가 봉 포함 ───────────────────────
class TestMinuteFetch:
    def test_window_and_closing_bar(self, monkeypatch):
        calls = {}

        def fake_minute_bars(symbols, start_dt, end_dt=None, adjustment='raw', chunk=100):
            calls.update(dict(symbols=symbols, start=start_dt, end=end_dt, adjustment=adjustment))
            ts = ['2026-10-19 15:59', '2026-10-20 09:29', '2026-10-20 09:30', '2026-10-20 15:59', '2026-10-20 16:00', '2026-10-20 16:01']
            t = [pd.Timestamp(x, tz='America/New_York').tz_convert('UTC') for x in ts]
            return pd.DataFrame({'symbol': 'X', 't': t, 'o': 1.0, 'h': 1.0, 'l': 1.0, 'c': [1, 2, 3, 4, 5, 6], 'v': 10, 'n': 1, 'vw': 1.0})

        monkeypatch.setattr(tracker.alpaca_data, 'minute_bars', fake_minute_bars)
        m = tracker.fetch_minute_data('X', '2026-10-19')
        assert calls['symbols'] == ['X'] and str(calls['start']) == '2026-10-20' and str(calls['end']) == '2026-10-27' and calls['adjustment'] == 'split'
        assert m['Close'].tolist() == [3, 4, 5]          # 진입일 봉·09:29·16:01 제외, 16:00 종가 봉 포함
        assert m.index.tz is None

    def test_expired_uses_closing_print(self):
        idx = pd.date_range('2026-10-20 09:30', '2026-10-20 16:00', freq='1min')
        m = pd.DataFrame({'Open': 10.0, 'High': 10.1, 'Low': 9.9, 'Close': 10.0}, index=idx)
        m.iloc[-1, m.columns.get_loc('Close')] = 10.3       # 16:00 종가 체결
        r = tracker.track_position_intraday(10.0, 10.5, 9.0, 1, m)
        assert r[0] == 'EXPIRED' and r[1] == 10.3


# ─── 최초 재계산 표시는 실패가 많아도 남긴다 (매일 반복 방지) ─────────────────
class TestMigrationMarker:
    def test_marker_written_even_with_many_failures(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(tracker, 'reverify_all', lambda: {'attempted': 100, 'failed': 40})
        tracker.migrate_to_v6()
        assert os.path.exists(tracker.V6_MARKER)
        assert 'failed=40' in open(tracker.V6_MARKER).read()
        monkeypatch.setattr(tracker, 'reverify_all', boom)
        tracker.migrate_to_v6()        # 두 번째는 재계산을 돌리지 않음


# ─── 청산된 행은 status='CLOSED' ─────────────────────────────────────────────
class TestClosedStatus:
    def test_closed_row_status(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        os.makedirs('data')
        row = {c: '' for c in tracker.OPEN_COLS}
        row.update({'strategy': '1', 'ticker': 'AAA', 'signal_date': '2026-10-19', 'signal_price': '10', 'entry_date': '2026-10-19',
                    'entry_price': '10', 'tp_price': '10.5', 'sl_price': '9.0', 'tp_pct': '0.05', 'max_hold': '2', 'status': 'OPEN',
                    'days_held': '0', 'method': tracker.METHOD_V6})
        pd.DataFrame([row]).to_csv('data/open_positions.csv', index=False)
        h = pd.DataFrame([[10, 10.2, 9.8, 10.0], [11, 11.2, 10.8, 11.0]], columns=['Open', 'High', 'Low', 'Close'],
                         index=pd.to_datetime(['2026-10-19', '2026-10-20']))
        monkeypatch.setattr(tracker, 'fetch_price_data', lambda t, s, e=None: h)
        monkeypatch.setattr(tracker, 'fetch_minute_data', lambda t, s: None)
        monkeypatch.setattr(tracker, 'drop_unfinished_bar', lambda x: x)
        tracker.update_open_positions()
        c = pd.read_csv('data/closed_positions.csv', dtype=str, keep_default_na=False)
        assert len(c) == 1 and c['status'].iloc[0] == 'CLOSED' and c['result_status'].iloc[0] == 'WIN' and c['judge'].iloc[0] == 'daily'
        assert float(c['close_price'].iloc[0]) == 11.0      # 다음 날 시가 갭 → 시가 체결
        o = pd.read_csv('data/open_positions.csv', dtype=str, keep_default_na=False)
        assert len(o) == 0


# ─── 예약이 여러 번 걸려 있어도 하루 한 번만 실제로 돈다 ─────────────────────
class TestIdempotentAfterClose:
    def test_last_session_waits_for_settle(self, monkeypatch):
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 16, 7))     # 16:07 — 종가 확정 전
        assert scanner._last_session_et() == '2026-10-16'
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 16, 25))
        assert scanner._last_session_et() == '2026-10-19'

    def test_already_scanned(self, tmp_path, monkeypatch):
        import json
        monkeypatch.chdir(tmp_path); os.makedirs('data/ohlcv')
        pd.DataFrame([{'ticker': 'AAA', 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1}]).to_parquet('data/ohlcv/2026-10-19.parquet', index=False)
        assert scanner.already_scanned('2026-10-19') is False                        # latest_scan.json 없음
        json.dump({'scan_date': '2026-10-19', 'data_source': 'alpaca', 'mode': 'preclose'}, open('data/latest_scan.json', 'w'))
        assert scanner.already_scanned('2026-10-19') is False                        # 마감 전 스캔 기록은 해당 없음
        json.dump({'scan_date': '2026-10-19', 'data_source': 'alpaca', 'mode': 'afterclose'}, open('data/latest_scan.json', 'w'))
        assert scanner.already_scanned('2026-10-19') is True
        assert scanner.already_scanned('2026-10-20') is False                        # 캐시가 아직 그날까지 없음

    def test_second_firing_exits_before_network(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(scanner, '_now_et', lambda: et(2026, 10, 19, 16, 45))
        monkeypatch.setattr(scanner.alpaca_data, 'have_keys', lambda: True)
        monkeypatch.setattr(scanner, 'already_scanned', lambda through: through == '2026-10-19')
        monkeypatch.setattr(scanner, 'update_cache', boom)
        monkeypatch.setattr(sys, 'argv', ['scanner.py'])
        scanner.main()
        assert '이미 끝남' in capsys.readouterr().out and not os.path.exists('/tmp/scanned_marker_never')
