#!/usr/bin/env python3
"""
한국투자증권 Open API (KIS Developers) — 미국 주식 주문용 최소 클라이언트
  출처: github.com/koreainvestment/open-trading-api (examples_llm/overseas_stock, kis_auth.py)

  키는 환경변수(GitHub Secrets)에서만 읽는다. 키·토큰·계좌번호·금액은 절대 출력하지 않는다
  (공개 저장소의 Actions 로그는 누구나 볼 수 있음).
    KIS_APP_KEY, KIS_APP_SECRET   앱키 / 앱시크릿
    KIS_ACCOUNT                   계좌번호 10자리 ("12345678-01" 또는 "1234567801")
    KIS_ENV                       real (기본) | demo (모의투자: 지정가 주문만 가능)

  쓰는 API
    POST /oauth2/tokenP                                      토큰 (24시간, 6시간 안 재발급은 같은 토큰)
    GET  /uapi/overseas-price/v1/quotations/price            현재가 (HHDFS00000300, EXCD=NAS/NYS/AMS)
    GET  /uapi/overseas-stock/v1/trading/inquire-balance     잔고 (TTTS3012R)
    GET  /uapi/overseas-stock/v1/trading/inquire-psamount    매수가능금액 (TTTS3007R)
    GET  /uapi/overseas-stock/v1/trading/inquire-nccs        미체결 (TTTS3018R)
    GET  /uapi/overseas-stock/v1/trading/inquire-ccnl        주문체결내역 (TTTS3035R, 날짜는 현지시각)
    POST /uapi/overseas-stock/v1/trading/order               주문 (매수 TTTT1002U / 매도 TTTT1006U)
    POST /uapi/overseas-stock/v1/trading/order-rvsecncl      정정·취소 (TTTT1004U)
  미국 주문구분(ORD_DVSN): 00 지정가 · 32 LOO · 34 LOC(매수·매도) · 31 MOO · 33 MOC(매도만). 시장가 매수 없음.
"""
import os
import json
import time
import math
import logging

import requests

URL_REAL = 'https://openapi.koreainvestment.com:9443'
URL_DEMO = 'https://openapivts.koreainvestment.com:29443'
EXCG_ORDER = {'NASDAQ': 'NASD', 'NYSE': 'NYSE', 'AMEX': 'AMEX', 'ARCA': 'AMEX', 'BATS': 'NASD'}
EXCG_PRICE = {'NASD': 'NAS', 'NYSE': 'NYS', 'AMEX': 'AMS'}


class KISError(RuntimeError):
    pass


def tick(price):
    """미국 주식 호가 단위: $1 이상 0.01, 미만 0.0001"""
    return 0.01 if price >= 1 else 0.0001


def round_px(price, up=False):
    t = tick(price)
    n = price / t
    n = math.ceil(n - 1e-9) if up else math.floor(n + 1e-9)
    return round(n * t, 2 if t == 0.01 else 4)


class KIS:
    def __init__(self, env=None, session=None):
        self.key = os.environ.get('KIS_APP_KEY', '').strip()
        self.secret = os.environ.get('KIS_APP_SECRET', '').strip()
        acct = os.environ.get('KIS_ACCOUNT', '').replace('-', '').strip()
        self.cano, self.prod = acct[:8], (acct[8:10] or '01')
        self.env = (env or os.environ.get('KIS_ENV', 'real')).strip().lower()
        self.base = URL_DEMO if self.env == 'demo' else URL_REAL
        self.s = session or requests.Session()
        self.token = None

    def ready(self):
        return bool(self.key and self.secret and len(self.cano) == 8)

    # ── 공통 ──
    def _tr(self, tr):
        return 'V' + tr[1:] if self.env == 'demo' and tr[0] in 'TJC' else tr

    def auth(self):
        if self.token:
            return self.token
        r = self.s.post(f'{self.base}/oauth2/tokenP', data=json.dumps({'grant_type': 'client_credentials', 'appkey': self.key, 'appsecret': self.secret}),
                        headers={'Content-Type': 'application/json'}, timeout=30)
        if r.status_code >= 400:
            raise KISError(f'토큰 발급 실패 {r.status_code}')        # 응답 본문은 출력하지 않음
        self.token = r.json().get('access_token')
        if not self.token:
            raise KISError('토큰 없음')
        return self.token

    def _headers(self, tr, tr_cont=''):
        return {'Content-Type': 'application/json', 'authorization': f'Bearer {self.auth()}', 'appkey': self.key,
                'appsecret': self.secret, 'tr_id': self._tr(tr), 'custtype': 'P', 'tr_cont': tr_cont}

    def _req(self, method, path, tr, params, tr_cont=''):
        last = ''
        for a in range(4):
            try:
                if method == 'GET':
                    r = self.s.get(self.base + path, headers=self._headers(tr, tr_cont), params=params, timeout=30)
                else:
                    r = self.s.post(self.base + path, headers=self._headers(tr, tr_cont), data=json.dumps(params), timeout=30)
            except requests.RequestException as e:
                last = type(e).__name__; time.sleep(2 * (a + 1)); continue
            if r.status_code in (429, 500, 502, 503, 504):
                last = str(r.status_code); time.sleep(2 * (a + 1)); continue
            try:
                j = r.json()
            except ValueError:
                raise KISError(f'{path} 응답 해석 실패 {r.status_code}')
            if str(j.get('rt_cd', '1')) != '0':
                msg = str(j.get('msg1', ''))[:80]
                if 'EGW00201' in str(j.get('msg_cd', '')) or '초당' in msg:     # 초당 거래건수 초과
                    time.sleep(1.1); last = msg; continue
                raise KISError(f'{path.split("/")[-1]} 거부: {msg} ({j.get("msg_cd", "")})')
            time.sleep(0.12)
            return j, r.headers.get('tr_cont', '')
        raise KISError(f'{path} 요청 실패({last})')

    # ── 시세 ──
    def price(self, symbol, excg):
        j, _ = self._req('GET', '/uapi/overseas-price/v1/quotations/price', 'HHDFS00000300',
                         {'AUTH': '', 'EXCD': EXCG_PRICE.get(excg, 'NAS'), 'SYMB': symbol})
        out = j.get('output') or {}
        try:
            return float(out.get('last') or 0) or None
        except ValueError:
            return None

    # ── 계좌 ──
    def balance(self):
        """보유 종목 [{symbol, qty, sellable, avg, price, excg}] — 미국 3개 거래소 합침"""
        rows, seen = [], set()
        for ex in ('NASD', 'NYSE', 'AMEX'):
            fk = nk = ''; cont = ''
            for _ in range(10):
                j, cont_h = self._req('GET', '/uapi/overseas-stock/v1/trading/inquire-balance', 'TTTS3012R',
                                      {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'OVRS_EXCG_CD': ex, 'TR_CRCY_CD': 'USD',
                                       'CTX_AREA_FK200': fk, 'CTX_AREA_NK200': nk}, cont)
                for o in j.get('output1') or []:
                    sym = o.get('ovrs_pdno', ''); qty = int(float(o.get('ovrs_cblc_qty') or 0))
                    if sym and qty > 0 and sym not in seen:
                        seen.add(sym)
                        rows.append({'symbol': sym, 'qty': qty, 'sellable': int(float(o.get('ord_psbl_qty') or 0)),
                                     'avg': float(o.get('pchs_avg_pric') or 0), 'price': float(o.get('now_pric2') or 0),
                                     'excg': o.get('ovrs_excg_cd') or ex})
                if cont_h not in ('F', 'M'):
                    break
                fk, nk, cont = j.get('ctx_area_fk200', ''), j.get('ctx_area_nk200', ''), 'N'
            if self.env == 'demo':
                break
        return rows

    def buying_power(self, symbol, excg, price):
        """외화 주문가능금액(USD)"""
        j, _ = self._req('GET', '/uapi/overseas-stock/v1/trading/inquire-psamount', 'TTTS3007R',
                         {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'OVRS_EXCG_CD': excg, 'OVRS_ORD_UNPR': f'{price}', 'ITEM_CD': symbol})
        o = j.get('output') or {}
        for k in ('ovrs_ord_psbl_amt', 'frcr_ord_psbl_amt1'):
            try:
                v = float(o.get(k) or 0)
                if v > 0:
                    return v
            except ValueError:
                pass
        return 0.0

    def present_balance(self):
        """체결기준 현재잔고(CTRP6504R) — 통화별 외화예수금 등. 반환: (output2 리스트, output3 dict)"""
        j, _ = self._req('GET', '/uapi/overseas-stock/v1/trading/inquire-present-balance', 'CTRP6504R',
                         {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'WCRC_FRCR_DVSN_CD': '02', 'NATN_CD': '840', 'TR_MKET_CD': '00', 'INQR_DVSN_CD': '00'})
        return j.get('output2') or [], j.get('output3') or {}

    def open_orders(self):
        """미체결 [{odno, symbol, side(buy/sell), qty_left, price, excg}]"""
        out = []
        for ex in ('NASD', 'NYSE', 'AMEX'):
            j, _ = self._req('GET', '/uapi/overseas-stock/v1/trading/inquire-nccs', 'TTTS3018R',
                             {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'OVRS_EXCG_CD': ex, 'SORT_SQN': 'DS', 'CTX_AREA_FK200': '', 'CTX_AREA_NK200': ''})
            for o in j.get('output') or []:
                left = int(float(o.get('nccs_qty') or 0))
                if left > 0:
                    out.append({'odno': o.get('odno'), 'symbol': o.get('pdno'), 'side': 'sell' if o.get('sll_buy_dvsn_cd') == '01' else 'buy',
                                'qty_left': left, 'price': float(o.get('ft_ord_unpr3') or 0), 'excg': o.get('ovrs_excg_cd') or ex})
        return out

    def fills(self, start_yyyymmdd, end_yyyymmdd):
        """체결 내역 [{date(현지), symbol, side, qty, price}]"""
        out = []; fk = nk = ''; cont = ''
        for _ in range(10):
            j, cont_h = self._req('GET', '/uapi/overseas-stock/v1/trading/inquire-ccnl', 'TTTS3035R',
                                  {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'PDNO': '%' if self.env != 'demo' else '',
                                   'ORD_STRT_DT': start_yyyymmdd, 'ORD_END_DT': end_yyyymmdd, 'SLL_BUY_DVSN': '00',
                                   'CCLD_NCCS_DVSN': '01' if self.env != 'demo' else '00', 'OVRS_EXCG_CD': '%' if self.env != 'demo' else '',
                                   'SORT_SQN': 'DS', 'ORD_DT': '', 'ORD_GNO_BRNO': '', 'ODNO': '', 'CTX_AREA_NK200': nk, 'CTX_AREA_FK200': fk}, cont)
            for o in j.get('output') or []:
                q = int(float(o.get('ft_ccld_qty') or 0))
                if q > 0:
                    out.append({'date': o.get('ord_dt', ''), 'symbol': o.get('pdno'), 'side': 'sell' if o.get('sll_buy_dvsn_cd') == '01' else 'buy',
                                'qty': q, 'price': float(o.get('ft_ccld_unpr3') or 0)})
            if cont_h not in ('F', 'M'):
                break
            fk, nk, cont = j.get('ctx_area_fk200', ''), j.get('ctx_area_nk200', ''), 'N'
        return out

    # ── 주문 ──
    def order(self, side, symbol, excg, qty, price, ord_dvsn):
        """side: buy|sell. price: 지정가(MOC 등 시장가류는 0). 반환: 주문번호"""
        tr = 'TTTT1002U' if side == 'buy' else ('TTTT1006U' if self.env != 'demo' else 'VTTT1001U')
        if self.env == 'demo':
            tr = 'VTTT1002U' if side == 'buy' else 'VTTT1001U'
            if ord_dvsn != '00':
                raise KISError('모의투자는 지정가(00)만 가능')
        j, _ = self._req('POST', '/uapi/overseas-stock/v1/trading/order', tr,
                         {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'OVRS_EXCG_CD': excg, 'PDNO': symbol, 'ORD_QTY': str(int(qty)),
                          'OVRS_ORD_UNPR': f'{price:.4f}' if price else '0', 'CTAC_TLNO': '', 'MGCO_APTM_ODNO': '',
                          'SLL_TYPE': '00' if side == 'sell' else '', 'ORD_SVR_DVSN_CD': '0', 'ORD_DVSN': ord_dvsn})
        return (j.get('output') or {}).get('ODNO', '')

    def cancel(self, odno, symbol, excg, qty):
        self._req('POST', '/uapi/overseas-stock/v1/trading/order-rvsecncl', 'TTTT1004U',
                  {'CANO': self.cano, 'ACNT_PRDT_CD': self.prod, 'OVRS_EXCG_CD': excg, 'PDNO': symbol, 'ORGN_ODNO': odno,
                   'RVSE_CNCL_DVSN_CD': '02', 'ORD_QTY': str(int(qty)), 'OVRS_ORD_UNPR': '0', 'MGCO_APTM_ODNO': '', 'ORD_SVR_DVSN_CD': '0'})
