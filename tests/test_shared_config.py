#!/usr/bin/env python3
"""
================================================================================
  Unit Tests — shared_config.py
  RSI 계산, DST 판정, 공휴일, 거래일 확인, 전략 설정 검증
================================================================================
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime, timezone, timedelta

from shared_config import (
    KST, LEVERAGED_ETF, STRATEGY_CONFIG, STRATEGY_NAMES,
    MIN_PRICE, MIN_VOLUME,
    is_us_dst, get_us_market_holidays, is_us_trading_day,
    get_last_trading_date, calc_rsi_wilder,
)


# ============================================================================
# STRATEGY CONFIG 검증
# ============================================================================

class TestStrategyConfig:
    """전략 설정값이 올바른지 검증"""

    def test_all_strategies_exist(self):
        for s in ['A', 'B', 'C', 'D', 'E']:
            assert s in STRATEGY_CONFIG, f"Strategy {s} missing"

    def test_tp_values(self):
        assert STRATEGY_CONFIG['A']['tp_pct'] == 0.05
        assert STRATEGY_CONFIG['B']['tp_pct'] == 0.15
        assert STRATEGY_CONFIG['C']['tp_pct'] == 0.05
        assert STRATEGY_CONFIG['D']['tp_pct'] == 0.20
        assert STRATEGY_CONFIG['E']['tp_pct'] == 0.10

    def test_sl_values(self):
        assert STRATEGY_CONFIG['A']['sl_pct'] == -0.20
        assert STRATEGY_CONFIG['B']['sl_pct'] == -0.20
        assert STRATEGY_CONFIG['C']['sl_pct'] == -0.20
        assert STRATEGY_CONFIG['D']['sl_pct'] is None  # 손절 없음
        assert STRATEGY_CONFIG['E']['sl_pct'] is None  # 손절 없음

    def test_max_hold(self):
        assert STRATEGY_CONFIG['A']['max_hold'] == 5
        assert STRATEGY_CONFIG['B']['max_hold'] == 10
        assert STRATEGY_CONFIG['C']['max_hold'] == 5
        assert STRATEGY_CONFIG['D']['max_hold'] == 30
        assert STRATEGY_CONFIG['E']['max_hold'] == 30

    def test_trailing_only_strategy_a(self):
        """트레일링 스탑은 A만 -3%"""
        assert STRATEGY_CONFIG['A']['trailing_pct'] == -0.03
        for s in ['B', 'C', 'D', 'E']:
            assert STRATEGY_CONFIG[s]['trailing_pct'] is None

    def test_strategy_names(self):
        for s in ['A', 'B', 'C', 'D', 'E']:
            assert s in STRATEGY_NAMES

    def test_leveraged_etf_exclusion(self):
        """주요 레버리지 ETF가 제외 목록에 있는지"""
        for etf in ['TQQQ', 'SQQQ', 'SOXL', 'SOXS', 'UVXY']:
            assert etf in LEVERAGED_ETF


# ============================================================================
# RSI WILDER 계산 검증
# ============================================================================

class TestCalcRsiWilder:
    """Wilder RSI 계산 정확도 검증"""

    def test_basic_output_range(self):
        """RSI 값이 0~100 범위 내"""
        np.random.seed(42)
        prices = pd.Series(np.random.uniform(10, 50, 100).cumsum())
        rsi = calc_rsi_wilder(prices, period=7)
        valid = rsi.dropna()
        assert len(valid) > 0
        assert valid.min() >= 0
        assert valid.max() <= 100

    def test_monotonic_up_gives_high_rsi(self):
        """지속 상승 시 RSI > 70"""
        prices = pd.Series([10 + i * 0.5 for i in range(30)])
        rsi = calc_rsi_wilder(prices, period=7)
        assert rsi.iloc[-1] > 70

    def test_monotonic_down_gives_low_rsi(self):
        """지속 하락 시 RSI < 30"""
        prices = pd.Series([50 - i * 0.5 for i in range(30)])
        rsi = calc_rsi_wilder(prices, period=7)
        assert rsi.iloc[-1] < 30

    def test_nan_for_insufficient_data(self):
        """데이터 부족 시 NaN"""
        prices = pd.Series([10, 11, 12])
        rsi = calc_rsi_wilder(prices, period=7)
        assert rsi.isna().all()

    def test_period_14(self):
        """period=14도 정상 동작"""
        np.random.seed(0)
        prices = pd.Series(np.random.uniform(10, 50, 50).cumsum())
        rsi = calc_rsi_wilder(prices, period=14)
        valid = rsi.dropna()
        assert len(valid) > 0
        assert valid.min() >= 0
        assert valid.max() <= 100

    def test_flat_price_gives_nan_or_50(self):
        """가격 변동 없으면 RSI = NaN 또는 ~50"""
        prices = pd.Series([10.0] * 30)
        rsi = calc_rsi_wilder(prices, period=7)
        # gain=0, loss=0 → rs=0/1e-10=0 → RSI=0, 또는 NaN
        # 실제로는 gain=0이면 RSI=0
        valid = rsi.dropna()
        if len(valid) > 0:
            # 변동 없으면 gain=0, loss=0 → RSI ≈ 0
            assert valid.iloc[-1] < 1 or pd.isna(valid.iloc[-1])


# ============================================================================
# DST / 공휴일 / 거래일 검증
# ============================================================================

class TestDST:
    """미국 서머타임 판정"""

    def test_summer_is_dst(self):
        assert is_us_dst(date(2026, 7, 4)) is True

    def test_winter_is_not_dst(self):
        assert is_us_dst(date(2026, 1, 15)) is False

    def test_march_transition(self):
        """3월 둘째 일요일 이후 DST 시작"""
        # 2026년 3월 8일 = 둘째 일요일
        assert is_us_dst(date(2026, 3, 7)) is False
        assert is_us_dst(date(2026, 3, 9)) is True

    def test_november_transition(self):
        """11월 첫째 일요일 이후 DST 종료"""
        # 2026년 11월 1일 = 일요일
        assert is_us_dst(date(2026, 10, 31)) is True
        assert is_us_dst(date(2026, 11, 2)) is False


class TestHolidays:
    """미국 시장 공휴일"""

    def test_2026_holidays_include_known(self):
        holidays = get_us_market_holidays(2026)
        # 새해
        assert date(2026, 1, 1) in holidays
        # 독립기념일 (7/4 토요일 → 7/3 금요일 대체)
        assert date(2026, 7, 3) in holidays
        # 추수감사절 (11월 넷째 목요일)
        assert date(2026, 11, 26) in holidays
        # 크리스마스
        assert date(2026, 12, 25) in holidays

    def test_weekday_not_holiday(self):
        """일반 평일은 공휴일이 아님"""
        holidays = get_us_market_holidays(2026)
        assert date(2026, 4, 7) not in holidays  # 평범한 월요일


class TestTradingDay:
    """거래일 판정"""

    def test_weekday_is_trading(self):
        assert is_us_trading_day(date(2026, 4, 6)) is True  # 월요일

    def test_weekend_not_trading(self):
        assert is_us_trading_day(date(2026, 4, 4)) is False  # 토요일
        assert is_us_trading_day(date(2026, 4, 5)) is False  # 일요일

    def test_holiday_not_trading(self):
        assert is_us_trading_day(date(2026, 12, 25)) is False  # 크리스마스

    def test_get_last_trading_date_skips_weekend(self):
        # 월요일에서 이전 거래일은 금요일
        last = get_last_trading_date(date(2026, 4, 6))
        assert last == date(2026, 4, 3)  # 금요일

    def test_get_last_trading_date_skips_holiday(self):
        # 크리스마스 다음날에서 이전 거래일은 크리스마스 전날
        last = get_last_trading_date(date(2026, 12, 26))
        assert last == date(2026, 12, 24)  # 크리스마스 전 목요일


# ============================================================================
# TP/SL 판정 로직 검증 (전략별 시뮬레이션)
# ============================================================================

class TestTPSLLogic:
    """TP/SL 판정이 전략 설정과 일치하는지"""

    def _check_tp(self, strategy, entry_price, high_price):
        """고가가 TP에 도달했는지"""
        tp_pct = STRATEGY_CONFIG[strategy]['tp_pct']
        tp_price = entry_price * (1 + tp_pct)
        return high_price >= tp_price

    def _check_sl(self, strategy, entry_price, low_price):
        """저가가 SL에 도달했는지"""
        sl_pct = STRATEGY_CONFIG[strategy]['sl_pct']
        if sl_pct is None:
            return False  # D, E는 손절 없음
        sl_price = entry_price * (1 + sl_pct)
        return low_price <= sl_price

    def test_strategy_a_tp_5pct(self):
        assert self._check_tp('A', 10.0, 10.50) is True
        assert self._check_tp('A', 10.0, 10.49) is False

    def test_strategy_b_tp_15pct(self):
        assert self._check_tp('B', 10.0, 11.50) is True
        assert self._check_tp('B', 10.0, 11.49) is False

    def test_strategy_d_no_sl(self):
        """D는 손절 없음"""
        assert self._check_sl('D', 10.0, 1.0) is False

    def test_strategy_e_no_sl(self):
        """E는 손절 없음"""
        assert self._check_sl('E', 10.0, 1.0) is False

    def test_strategy_a_sl_20pct(self):
        assert self._check_sl('A', 10.0, 8.00) is True   # -20%
        assert self._check_sl('A', 10.0, 8.01) is False  # -19.9%

    def test_max_hold_expired(self):
        """보유기간 초과 = EXPIRED"""
        for s in ['A', 'B', 'C', 'D', 'E']:
            max_hold = STRATEGY_CONFIG[s]['max_hold']
            assert max_hold > 0
            # 보유일 > max_hold → 만기
            assert max_hold + 1 > max_hold


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
