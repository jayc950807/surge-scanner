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
    def test_next_day_open(self, monkeypatch):
        h = bars([[9, 9.5, 8.5, 9.0], [9.3, 9.9, 9.1, 9.8]], start='2026-03-02')
        monkeypatch.setattr(tracker, 'fetch_price_data', lambda t, s, e=None: h)
        d, p, c = tracker.get_entry_for_signal('X', '2026-03-02')
        assert d == '2026-03-03' and p == 9.3 and c == 9.0

    def test_no_next_day_yet(self, monkeypatch):
        h = bars([[9, 9.5, 8.5, 9.0]], start='2026-03-02')
        monkeypatch.setattr(tracker, 'fetch_price_data', lambda t, s, e=None: h)
        d, p, c = tracker.get_entry_for_signal('X', '2026-03-02')
        assert d is None and p is None and c == 9.0

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
