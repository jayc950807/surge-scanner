#!/usr/bin/env python3
"""자동 매매 봇 테스트 — 가짜 한국투자증권으로 주문 흐름 확인 (네트워크 없음)"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
from datetime import datetime, date, timedelta, timezone
import pandas as pd
import pytest

import bot
from kis import KISError, round_px

ET = timezone(timedelta(hours=-4))


class FakeKIS:
    def __init__(self, hold=(), prices=None, fills=(), cash=10000.0, reject_loc=False, reject_moc=False):
        self.hold = [dict(h) for h in hold]; self.prices = prices or {}; self._fills = list(fills)
        self._cash = cash; self.orders = []; self.cancels = []; self.reject_loc = reject_loc; self.reject_moc = reject_moc
        self.open = []

    def ready(self): return True
    def balance(self): return [dict(h) for h in self.hold]
    def open_orders(self): return list(self.open)
    def price(self, s, ex): return self.prices.get(s)
    def buying_power(self, s, ex, px): return self._cash
    def fills(self, a, b): return list(self._fills)

    def order(self, side, sym, ex, qty, px, dvsn):
        if dvsn == '34' and self.reject_loc: raise KISError('LOC 불가')
        if dvsn == '33' and self.reject_moc: raise KISError('MOC 불가')
        self.orders.append((side, sym, qty, px, dvsn)); return str(len(self.orders))

    def cancel(self, odno, sym, ex, qty): self.cancels.append(odno)


@pytest.fixture
def live(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path); os.makedirs('data/ohlcv')
    pd.DataFrame([{'symbol': 'AAA', 'name': 'A', 'exchange': 'NASDAQ'}, {'symbol': 'BBB', 'name': 'B', 'exchange': 'NYSE'}]).to_csv('data/ohlcv/universe.csv', index=False)
    monkeypatch.setattr(bot, 'MODE', 'live'); monkeypatch.setattr(bot, 'PAUSE', False)
    monkeypatch.setattr(bot.time, 'sleep', lambda s: None)
    return tmp_path


def signals(rows, day='2026-10-22'):
    pd.DataFrame(rows).to_csv(f'data/signal_{day}.csv', index=False)


NOW = datetime(2026, 10, 22, 15, 46, tzinfo=ET)    # 목요일


class TestPreclose:
    def test_buys_loc_with_cap_and_sizing(self, live, monkeypatch):
        monkeypatch.setattr(bot, 'SIZING', 'pct')
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'},
                 {'strategy': '3', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'},
                 {'strategy': '1', 'ticker': 'BBB', 'date': '2026-10-22', 'scan_type': 'afterclose', 'price_1530': ''}])
        k = FakeKIS(prices={'AAA': 2.10}, cash=10000.0)
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('buy', 'AAA', 460, 2.17, '34')]        # 계좌 10000 의 10% = 1000 / 한도 2.17(2.10×1.03 올림) = 460주, 마감 뒤 신호(BBB)는 안 삼

    def test_loc_rejected_falls_back_to_limit(self, live, monkeypatch):
        monkeypatch.setattr(bot, 'SIZING', 'pct')
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(prices={'AAA': 2.00}, reject_loc=True)
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('buy', 'AAA', 485, 2.02, '00')]

    def test_split_cash_equally_by_signal_count(self, live):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'},
                 {'strategy': '3', 'ticker': 'BBB', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '4.00'}])
        k = FakeKIS(prices={'AAA': 2.00, 'BBB': 4.00}, cash=1000.0)
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('buy', 'AAA', 242, 2.06, '34'), ('buy', 'BBB', 121, 4.12, '34')]   # 1000 ÷ 2 = 500씩 (한도가 +3%)

    def test_split_one_signal_uses_all_cash(self, live):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(prices={'AAA': 2.00}, cash=72.0)
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('buy', 'AAA', 34, 2.06, '34')]                # 72 전부 → 34주

    def test_split_drops_unaffordable_and_redistributes(self, live):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'},
                 {'strategy': '1', 'ticker': 'BBB', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '50.00'}])
        k = FakeKIS(prices={'AAA': 2.00, 'BBB': 50.00}, cash=60.0)
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('buy', 'AAA', 29, 2.06, '34')]                # 30씩이면 BBB(51.5) 못 삼 → 빼고 AAA 에 60 전부

    def test_split_uses_only_cash_keeps_holdings(self, live):
        hold = [{'symbol': 'ZZZ', 'qty': 10, 'sellable': 10, 'avg': 5.0, 'price': 5.0, 'excg': 'NASD'}]
        k = FakeKIS(hold=hold, fills=[{'date': '20261022', 'symbol': 'ZZZ', 'side': 'buy', 'qty': 10, 'price': 5.0}], prices={'AAA': 2.0, 'BBB': 2.0}, cash=40.0)
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'},
                 {'strategy': '1', 'ticker': 'BBB', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('buy', 'AAA', 9, 2.06, '34'), ('buy', 'BBB', 9, 2.06, '34')]      # 현금 40 ÷ 2 = 20씩, ZZZ 는 그대로

    def test_day2_exit_moc_and_slots(self, live):
        # AAA: 화(10-20) 종가 매수 → 수=1일, 목=2일 → 목요일 마감에 MOC. BBB: 수(10-21) 매수 → 목=1일 → 보유
        hold = [{'symbol': 'AAA', 'qty': 100, 'sellable': 100, 'avg': 2.0, 'price': 2.05, 'excg': 'NASD'},
                {'symbol': 'BBB', 'qty': 50, 'sellable': 50, 'avg': 4.0, 'price': 4.1, 'excg': 'NYSE'}]
        fills = [{'date': '20261020', 'symbol': 'AAA', 'side': 'buy', 'qty': 100, 'price': 2.0},
                 {'date': '20261021', 'symbol': 'BBB', 'side': 'buy', 'qty': 50, 'price': 4.0}]
        k = FakeKIS(hold=hold, fills=fills, prices={'AAA': 2.05, 'BBB': 4.1})
        k.open = [{'odno': '9', 'symbol': 'AAA', 'side': 'sell', 'qty_left': 100, 'price': 2.1, 'excg': 'NASD'}]
        signals([{'strategy': '1', 'ticker': 'BBB', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '4.1'}])
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.cancels == ['9']                                        # 걸어 둔 익절 주문 취소 후
        assert ('sell', 'AAA', 100, 0, '33') in k.orders                 # 2일째 → 만기 MOC
        assert not any(o[1] == 'BBB' and o[0] == 'sell' for o in k.orders)   # 1일째 → 계속 보유
        assert not any(o[0] == 'buy' for o in k.orders)                  # BBB 이미 보유 → 안 삼

    def test_moc_rejected_falls_back(self, live):
        hold = [{'symbol': 'AAA', 'qty': 10, 'sellable': 10, 'avg': 2.0, 'price': 2.0, 'excg': 'NASD'}]
        k = FakeKIS(hold=hold, fills=[{'date': '20261020', 'symbol': 'AAA', 'side': 'buy', 'qty': 10, 'price': 2.0}], prices={'AAA': 2.0}, reject_moc=True)
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == [('sell', 'AAA', 10, 1.94, '00')]

    def test_outside_window_does_nothing(self, live):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(prices={'AAA': 2.0})
        bot.run_preclose(bot.Broker(k), datetime(2026, 10, 22, 15, 51, tzinfo=ET))
        assert k.orders == []

    def test_pause_and_drawdown_block_buys(self, live, monkeypatch):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        monkeypatch.setattr(bot, 'PAUSE', True)
        k = FakeKIS(prices={'AAA': 2.0}); bot.run_preclose(bot.Broker(k), NOW); assert k.orders == []
        monkeypatch.setattr(bot, 'PAUSE', False); monkeypatch.setattr(bot, 'START_EQ', 30000.0)
        k = FakeKIS(prices={'AAA': 2.0}, cash=10000); bot.run_preclose(bot.Broker(k), NOW); assert k.orders == []   # −67% < −60% 한도

    def test_live_check_stops_after_20_negative(self, live, monkeypatch):
        monkeypatch.setattr(bot, 'START_DATE', '2026-10-01')
        pd.DataFrame([{'ticker': f'T{i}', 'signal_date': '2026-10-05', 'scan_type': 'preclose', 'result_pct': '-1'} for i in range(20)]).to_csv('data/closed_positions.csv', index=False)
        assert bot.live_check()[0] is False

    def test_max_positions(self, live, monkeypatch):
        monkeypatch.setattr(bot, 'MAX_POS', 1)
        hold = [{'symbol': 'ZZZ', 'qty': 1, 'sellable': 1, 'avg': 2.0, 'price': 2.0, 'excg': 'NASD'}]
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(hold=hold, fills=[{'date': '20261022', 'symbol': 'ZZZ', 'side': 'buy', 'qty': 1, 'price': 2.0}], prices={'AAA': 2.0})
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == []

    def test_no_double_buy_when_order_pending(self, live):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(prices={'AAA': 2.0})
        k.open = [{'odno': '5', 'symbol': 'AAA', 'side': 'buy', 'qty_left': 100, 'price': 2.06, 'excg': 'NASD'}]
        bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == []

    def test_dry_mode_sends_nothing(self, live, monkeypatch):
        monkeypatch.setattr(bot, 'MODE', 'dry')
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(prices={'AAA': 2.0}); bot.run_preclose(bot.Broker(k), NOW)
        assert k.orders == []


class TestMonitor:
    def test_take_profit_orders(self, live):
        hold = [{'symbol': 'AAA', 'qty': 100, 'sellable': 100, 'avg': 2.0, 'price': 2.0, 'excg': 'NASD'},
                {'symbol': 'BBB', 'qty': 10, 'sellable': 10, 'avg': 0.8, 'price': 0.8, 'excg': 'NYSE'}]
        k = FakeKIS(hold=hold)
        k.open = [{'odno': '1', 'symbol': 'BBB', 'side': 'sell', 'qty_left': 10, 'price': 0.84, 'excg': 'NYSE'}]
        bot.place_take_profits(bot.Broker(k))
        assert k.orders == [('sell', 'AAA', 100, 2.1, '00')]             # BBB 는 이미 걸려 있음

    def test_take_profit_gap_up_sells_near_market(self, live):
        hold = [{'symbol': 'AAA', 'qty': 2, 'sellable': 2, 'avg': 2.70, 'price': 3.9, 'excg': 'NASD'}]
        k = FakeKIS(hold=hold, prices={'AAA': 3.92})
        bot.place_take_profits(bot.Broker(k))
        assert k.orders == [('sell', 'AAA', 2, 3.88, '00')]           # 목표(2.84)보다 위 → 현재가 −1%

    def test_take_profit_retried_when_missing(self, live):
        hold = [{'symbol': 'AAA', 'qty': 2, 'sellable': 2, 'avg': 2.0, 'price': 2.0, 'excg': 'NASD'}]
        k = FakeKIS(hold=hold, prices={'AAA': 2.0})
        bot.place_take_profits(bot.Broker(k)); k.open = [{'odno': '1', 'symbol': 'AAA', 'side': 'sell', 'qty_left': 2, 'price': 2.1, 'excg': 'NASD'}]
        bot.place_take_profits(bot.Broker(k))
        assert k.orders == [('sell', 'AAA', 2, 2.1, '00')]             # 이미 걸려 있으면 다시 안 검

    def test_stop_loss(self, live):
        hold = [{'symbol': 'AAA', 'qty': 100, 'sellable': 0, 'avg': 2.0, 'price': 1.79, 'excg': 'NASD'},
                {'symbol': 'BBB', 'qty': 10, 'sellable': 10, 'avg': 4.0, 'price': 3.7, 'excg': 'NYSE'}]
        k = FakeKIS(hold=hold, prices={'AAA': 1.79, 'BBB': 3.7})
        k.open = [{'odno': '7', 'symbol': 'AAA', 'side': 'sell', 'qty_left': 100, 'price': 2.1, 'excg': 'NASD'}]
        sold = []
        orig = k.order
        def order(side, sym, ex, qty, px, dvsn):
            r = orig(side, sym, ex, qty, px, dvsn); k.hold = [h for h in k.hold if h['symbol'] != sym]; return r
        k.order = order
        bot.check_stops(bot.Broker(k))
        assert k.cancels == ['7'] and k.orders == [('sell', 'AAA', 100, 1.73, '00')]   # −10.5% → 취소 후 현재가 −3%; BBB(−7.5%)는 유지

    def test_round_px(self):
        assert round_px(2.163, up=True) == 2.17 and round_px(2.169) == 2.16 and round_px(0.51234) == 0.5123


class TestLiveRecord:
    def test_order_recorded_then_closed(self, live, monkeypatch):
        signals([{'strategy': '1', 'ticker': 'AAA', 'date': '2026-10-22', 'scan_type': 'preclose', 'price_1530': '2.00'}])
        k = FakeKIS(prices={'AAA': 2.00}, cash=100.0)
        bot.run_preclose(bot.Broker(k), NOW)
        d = pd.read_csv('data/live_trades.csv', dtype=str)
        assert list(d.symbol) == ['AAA'] and d.status[0] == 'ordered' and 95 < float(d.weight_pct[0]) <= 100
        k2 = FakeKIS(fills=[{'date': '20261022', 'symbol': 'AAA', 'side': 'buy', 'qty': 48, 'price': 2.00},
                            {'date': '20261023', 'symbol': 'AAA', 'side': 'sell', 'qty': 48, 'price': 2.10}])
        monkeypatch.setattr(bot, 'now_et', lambda: datetime(2026, 10, 23, 16, 30, tzinfo=ET))
        bot.run_record(bot.Broker(k2))
        d = pd.read_csv('data/live_trades.csv', dtype=str)
        assert d.status[0] == 'closed' and d.result[0] == '익절' and abs(float(d.ret_pct[0]) - 5.0) < 0.01
        assert d.qty[0] == '48' and abs(float(d.pnl_usd[0]) - 4.8) < 0.01   # 2026-10-27 사용자 결정: 금액·수량 공개 기록
        a = pd.read_csv('data/live_account.csv'); assert list(a.columns) == ['date', 'equity_usd', 'cash_usd', 'stock_usd'] and len(a) == 1

    def test_unfilled_and_holding(self, live, monkeypatch):
        bot.live_record_order('2026-10-22', 'AAA', 50.0, 2.06); bot.live_record_order('2026-10-22', 'BBB', 50.0, 4.12)
        k = FakeKIS(hold=[{'symbol': 'BBB', 'qty': 10, 'sellable': 10, 'avg': 4.0, 'price': 4.2, 'excg': 'NYSE'}], prices={'BBB': 4.2},
                    fills=[{'date': '20261022', 'symbol': 'BBB', 'side': 'buy', 'qty': 10, 'price': 4.0}])
        monkeypatch.setattr(bot, 'now_et', lambda: datetime(2026, 10, 23, 16, 30, tzinfo=ET))
        bot.run_record(bot.Broker(k))
        d = pd.read_csv('data/live_trades.csv', dtype=str).set_index('symbol')
        assert d.at['AAA', 'status'] == 'unfilled' and d.at['BBB', 'status'] == 'holding' and abs(float(d.at['BBB', 'cur_pct']) - 5.0) < 0.01
