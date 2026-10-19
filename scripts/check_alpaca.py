#!/usr/bin/env python3
"""GitHub Secrets 에 넣은 Alpaca 키가 동작하는지 확인 (키 값은 출력하지 않음). 실패하면 종료 코드 1."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'core'))
from datetime import timedelta
import alpaca_data as ad

ok = True
def step(name, fn):
    global ok
    try:
        print(f"[{name}] {fn()}")
    except Exception as e:
        ok = False
        print(f"[{name}] 실패: {str(e)[:200]}")

if not ad.have_keys():
    print("✗ ALPACA_KEY / ALPACA_SECRET 이 비어 있음 — Secrets 이름이 정확한지 확인"); sys.exit(1)
k, _ = ad._key_pair()
print(f"키 ID 길이 {len(k)}자, 앞 2자 '{k[:2]}' (값은 표시하지 않음)")
step("거래 API clock", lambda: f"is_open={ad.clock().get('is_open')} next_close={ad.clock().get('next_close')} host={ad.TRADE_URL}")
step("종목 목록", lambda: f"{len(ad.assets('active'))}개 active")
today = ad.now_ny().date()
step("SPY 일봉(7일)", lambda: ad.history_daily('SPY', today - timedelta(days=10))[['Close']].tail(3).to_string())
step("SPY 1분봉(지연 SIP)", lambda: (lambda m: f"{len(m)}봉, 마지막 {m.index[-1]}")(ad.history_minute('SPY', today - timedelta(days=4), None, regular_only=False)))
step("기업행위(분할, 30일)", lambda: f"{len(ad.corporate_actions(today - timedelta(days=30), today + timedelta(days=10)))}건")
print("\n결과:", "✓ 키 정상 — 스캐너·추적기가 쓸 수 있음" if ok else "✗ 문제 있음 (위 실패 항목 참고)")
sys.exit(0 if ok else 1)
