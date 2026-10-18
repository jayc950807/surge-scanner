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
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
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
    """v5: 전략 1·3만 운영"""

    def test_only_active_strategies(self):
        assert set(STRATEGY_CONFIG) == {'1', '3'}
        assert set(STRATEGY_NAMES) == {'1', '3'}

    def test_values(self):
        for s in ('1', '3'):
            assert STRATEGY_CONFIG[s]['tp_pct'] == 0.05
            assert STRATEGY_CONFIG[s]['sl_pct'] == -0.20
            assert STRATEGY_CONFIG[s]['max_hold'] == 5

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
        return high_price >= entry_price * (1 + STRATEGY_CONFIG[strategy]['tp_pct'])

    def _check_sl(self, strategy, entry_price, low_price):
        return low_price <= entry_price * (1 + STRATEGY_CONFIG[strategy]['sl_pct'])

    def test_tp_5pct(self):
        for s in ('1', '3'):
            assert self._check_tp(s, 10.0, 10.50) is True
            assert self._check_tp(s, 10.0, 10.49) is False

    def test_sl_20pct(self):
        for s in ('1', '3'):
            assert self._check_sl(s, 10.0, 8.00) is True
            assert self._check_sl(s, 10.0, 8.01) is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
