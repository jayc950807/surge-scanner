#!/usr/bin/env python3
"""v5 추적 규칙 테스트 — 다음날 시가 진입, 갭 체결, 비용 차감, 신호 판정"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
import numpy as np
import pandas as pd
import pytest

import tracker
import scanner
from shared_config import COST_PCT


def bars(rows, start='2026-01-05'):
    idx = pd.bdate_range(start, periods=len(rows))
    return pd.DataFrame(rows, columns=['Open', 'High', 'Low', 'Close'], index=idx)


class TestTrackDaywise:
    def test_tp_on_entry_day(self):
        # 진입일(첫 행) 시가 10 매수, 그날 고가 10.6 → +5% 익절 (진입일도 판정에 포함)
        h = bars([[10, 10.6, 9.9, 10.2]])
        r = tracker.track_position_daywise(10, 10.5, 8.0, 5, h)
        assert r[0] == 'WIN' and r[1] == 10.5 and abs(r[9] - 5.0) < 1e-9

    def test_gap_down_fills_at_open(self):
        # 둘째 날 시가 7 (손절가 8 아래로 갭) → 시가 7 체결, -30%
        h = bars([[10, 10.2, 9.5, 9.6], [7, 7.5, 6.5, 7.2]])
        r = tracker.track_position_daywise(10, 10.5, 8.0, 5, h)
        assert r[0] == 'LOSS' and r[1] == 7 and abs(r[9] + 30.0) < 1e-9

    def test_gap_up_fills_at_open(self):
        h = bars([[10, 10.2, 9.5, 9.6], [11, 11.5, 10.8, 11.2]])
        r = tracker.track_position_daywise(10, 10.5, 8.0, 5, h)
        assert r[0] == 'WIN' and r[1] == 11 and abs(r[9] - 10.0) < 1e-9

    def test_same_day_both_is_loss(self):
        h = bars([[10, 10.6, 7.9, 9.0]])
        r = tracker.track_position_daywise(10, 10.5, 8.0, 5, h)
        assert r[0] == 'LOSS'

    def test_expired_close(self):
        h = bars([[10, 10.3, 9.6, 10.1]] * 5)
        r = tracker.track_position_daywise(10, 10.5, 8.0, 5, h)
        assert r[0] == 'EXPIRED' and r[8] == 5

    def test_pending_until_more_data(self):
        h = bars([[10, 10.3, 9.6, 10.1]] * 3)
        r = tracker.track_position_daywise(10, 10.5, 8.0, 5, h)
        assert r[0] is None


class TestEntry:
    def test_signal_day_close(self, monkeypatch):
        # v6: 신호일 종가(마감가 주문)로 진입
        h = bars([[9, 9.5, 8.5, 9.0], [9.3, 9.9, 9.1, 9.8]], start='2026-03-02')
        monkeypatch.setattr(tracker, 'fetch_price_data', lambda t, s, e=None: h)
        d, p, c = tracker.get_entry_for_signal('X', '2026-03-02')
        assert d == '2026-03-02' and p == 9.0 and c == 9.0

    def test_no_signal_day_bar_yet(self, monkeypatch):
        h = bars([[9, 9.5, 8.5, 9.0]], start='2026-02-27')   # 신호일(03-02) 봉이 아직 없음
        monkeypatch.setattr(tracker, 'fetch_price_data', lambda t, s, e=None: h)
        d, p, c = tracker.get_entry_for_signal('X', '2026-03-02')
        assert d is None and p is None

    def test_cost(self):
        assert tracker.net_of_cost(5.0) == 5.0 - COST_PCT


class TestSignals:
    def _df(self, closes, last_hl):
        n = len(closes)
        c = np.array(closes, float)
        df = pd.DataFrame({'Open': c * 1.01, 'High': c * 1.02, 'Low': c * 0.98, 'Close': c,
                           'Volume': np.full(n, 50000.0)}, index=pd.bdate_range('2026-01-01', periods=n))
        df.iloc[-1, df.columns.get_loc('High')] = last_hl[0]
        df.iloc[-1, df.columns.get_loc('Low')] = last_hl[1]
        return df

    def test_strategy_1_and_3_fire(self):
        # 30일 횡보 후 7일 연속 하락, 마지막 날 -10%, 일중 변동 25%, 종가가 5일 저점 근처
        closes = [20.0] * 30 + [19, 18, 17, 16, 15, 14, 12.6]
        df = self._df(closes, (15.2, 12.4))
        got = {s for s, _ in scanner.check_signals(df)}
        assert got == {'1', '3'}

    def test_no_signal_when_flat(self):
        df = self._df([20.0] * 40, (20.4, 19.6))
        assert scanner.check_signals(df) == []

    def test_price_floor(self):
        closes = [2.0] * 30 + [1.9, 1.8, 1.7, 1.6, 1.4, 1.2, 0.9]
        df = self._df(closes, (1.2, 0.85))
        assert scanner.check_signals(df) == []


class TestIntraday:
    def _mins(self, day, rows):
        idx = pd.date_range(f'{day} 09:30', periods=len(rows), freq='1min')
        return pd.DataFrame(rows, columns=['Open', 'High', 'Low', 'Close'], index=idx)

    def test_same_day_tp_first_is_win(self):
        # 일봉으로는 ±둘 다 닿아 LOSS 지만, 분봉으로는 +5%가 먼저 → WIN
        m = self._mins('2026-03-03', [[10, 10.6, 9.9, 10.5]] + [[10.5, 10.5, 8.8, 8.9]] * 3)
        r = tracker.track_position_intraday(10, 10.5, 9.0, 2, m)
        assert r[0] == 'WIN' and r[1] == 10.5

    def test_gap_down_second_day(self):
        m1 = self._mins('2026-03-03', [[10, 10.2, 9.7, 9.8]] * 390)
        m2 = self._mins('2026-03-04', [[8.5, 8.6, 8.4, 8.5]] * 5)
        r = tracker.track_position_intraday(10, 10.5, 9.0, 2, pd.concat([m1, m2]))
        assert r[0] == 'LOSS' and r[1] == 8.5 and r[8] == 2

    def test_still_open_when_day_unfinished(self):
        m = self._mins('2026-03-03', [[10, 10.2, 9.7, 9.8]] * 60)
        r = tracker.track_position_intraday(10, 10.5, 9.0, 1, m)
        assert r[0] is None and r[8] == 0                 # 끝나지 않은 날은 보유일수에 안 셈

    def test_days_held_counts_finished_days_only(self):
        m1 = self._mins('2026-03-03', [[10, 10.2, 9.7, 9.8]] * 390)
        m2 = self._mins('2026-03-04', [[9.9, 10.1, 9.7, 9.9]] * 5)       # 둘째 날 장중
        r = tracker.track_position_intraday(10, 10.5, 9.0, 2, pd.concat([m1, m2]), finished_days=[pd.Timestamp('2026-03-03')])
        assert r[0] is None and r[8] == 1

    def test_expired_at_last_close(self):
        m = self._mins('2026-03-03', [[10, 10.2, 9.7, 9.8]] * 390)
        r = tracker.track_position_intraday(10, 10.5, 9.0, 1, m)
        assert r[0] == 'EXPIRED'

    def test_expired_on_early_close_day_with_finished_days(self):
        # 15:55 봉이 없어도(조기 마감·거래 없음) 그날 확정 일봉이 있으면 만기 처리
        m1 = self._mins('2026-03-03', [[10, 10.2, 9.7, 9.8]] * 200)
        m2 = self._mins('2026-03-04', [[9.9, 10.1, 9.7, 9.9]] * 200)
        fin = [pd.Timestamp('2026-03-03'), pd.Timestamp('2026-03-04')]
        r = tracker.track_position_intraday(10, 10.5, 9.0, 2, pd.concat([m1, m2]), finished_days=fin)
        assert r[0] == 'EXPIRED' and r[8] == 2

    def test_minute_window_must_start_at_first_tracking_day(self):
        hist = pd.DataFrame({'Close': [1, 1]}, index=pd.to_datetime(['2026-03-03', '2026-03-04']))
        late = self._mins('2026-03-10', [[10, 10.2, 9.7, 9.8]] * 5)
        ok = self._mins('2026-03-03', [[10, 10.2, 9.7, 9.8]] * 5)
        assert tracker.minute_data_valid(late, hist) is False
        assert tracker.minute_data_valid(ok, hist) is True

    def test_daily_fallback_gap_on_first_day_for_v6(self):
        # 종가 진입 뒤 다음날 시가가 손절가 아래로 갭 → 그 시가로 체결
        h = bars([[8.0, 8.5, 7.9, 8.2]], start='2026-03-03')
        r = tracker.track_position_daywise(10, 10.5, 9.0, 2, h, gap_from_first_bar=True)
        assert r[0] == 'LOSS' and r[1] == 8.0
