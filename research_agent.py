"""
research_agent.py — 자율 주식 연구 에이전트 v2
================================================
Claude API를 "두뇌"로, yfinance를 "눈"으로 사용.

v2 핵심 변경:
- 단일 가설 → 매 사이클 다중 가설 동시 탐색
- Phase 구조: 발산(탐색) → 수렴(검증) → 심화(최적화)
- 자기 비판 메커니즘: 매 사이클마다 "이건 왜 틀릴 수 있는가" 검증
- 누적 지식 그래프: 발견 간 연결 관계 추적
- 전략 후보 파이프라인: 발견 → 가설 → 백테스트 → 최적화 → 최종 전략

Output:
  data/research_report.md           — 최종 종합 리포트
  data/research_log.json            — 전체 연구 과정 로그
  data/research_knowledge.json      — 누적 지식 그래프
  data/research_strategies.json     — 발견된 전략 후보
  data/research_datasets/*.csv      — 각 사이클 데이터
"""

import os, sys, json, time, logging, warnings, traceback, random
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import anthropic
import yfinance as yf
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("research_agent.log", mode="w"),
    ],
)
log = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────
MAX_CYCLES       = int(os.environ.get("MAX_CYCLES", 8))
MODEL            = "claude-sonnet-4-20250514"
OUTPUT_DIR       = Path("data")
DATASET_DIR      = OUTPUT_DIR / "research_datasets"
TICKER_CACHE     = None
PRICE_CACHE      = {}

client = anthropic.Anthropic()

# ═══════════════════════════════════════════════════════
#  지식 구조체
# ═══════════════════════════════════════════════════════

knowledge = {
    "facts": [],          # 확인된 사실 [{"id": "F1", "text": ..., "confidence": 0.9, "source": "cycle3"}]
    "hypotheses": [],     # 검증 대기 가설
    "dead_ends": [],      # 폐기된 방향 (재탐색 방지)
    "strategies": [],     # 발견된 전략 후보
    "open_questions": [], # 아직 답 못 찾은 질문
    "connections": [],    # 발견 간 연결 관계
}


# ═══════════════════════════════════════════════════════
#  데이터 도구
# ═══════════════════════════════════════════════════════

def get_all_tickers():
    global TICKER_CACHE
    if TICKER_CACHE is not None:
        return TICKER_CACHE
    tickers = set()
    import urllib.request
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        tickers.update(tables[0]['Symbol'].str.replace('.', '-', regex=False).tolist())
    except: pass
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        data = urllib.request.urlopen(url, timeout=30).read().decode()
        for line in data.strip().split('\n'):
            t = line.strip()
            if t and len(t) <= 5 and t.isalpha():
                tickers.add(t)
    except: pass
    TICKER_CACHE = sorted(tickers)
    log.info(f"티커 캐시: {len(TICKER_CACHE)}개")
    return TICKER_CACHE

def download_prices(tickers, start, end):
    cache_key = f"{hash(tuple(sorted(tickers[:10])))}_{start}_{end}"
    if cache_key in PRICE_CACHE:
        return PRICE_CACHE[cache_key]
    all_data = {}
    batch_size = 80
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        try:
            data = yf.download(" ".join(batch), start=start, end=end,
                              group_by='ticker', progress=False, threads=True)
            if data.empty: continue
            for t in batch:
                try:
                    df = data[t].copy() if len(batch) > 1 and t in data.columns.get_level_values(0) else (data.copy() if len(batch) == 1 else None)
                    if df is None: continue
                    df = df.dropna(subset=['Close','Open','High','Low','Volume'])
                    if len(df) >= 20: all_data[t] = df
                except: continue
        except: continue
    PRICE_CACHE[cache_key] = all_data
    return all_data

def sample_tickers(n=500):
    """전체에서 무작위 샘플링 (대형+중형+소형 고르게)"""
    all_t = get_all_tickers()
    if len(all_t) <= n: return all_t
    return random.sample(all_t, n)

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_g = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_l = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_g / avg_l.replace(0, np.nan)
    return 100 - 100 / (1 + rs)

def calc_bollinger(close, period=20):
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper, lower = ma + 2*std, ma - 2*std
    width = (upper - lower) / ma * 100
    pos = (close - lower) / (upper - lower)
    return ma, upper, lower, width, pos

def calc_atr(high, low, close, period=14):
    tr = pd.concat([high-low, (high-close.shift(1)).abs(), (low-close.shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_macd(close):
    ema12, ema26 = close.ewm(span=12).mean(), close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    return macd, signal, macd - signal

def add_all_indicators(df):
    c, h, l, v, o = df['Close'], df['High'], df['Low'], df['Volume'], df['Open']
    df['ret_1d'] = c.pct_change()*100; df['ret_3d'] = c.pct_change(3)*100
    df['ret_5d'] = c.pct_change(5)*100; df['ret_10d'] = c.pct_change(10)*100; df['ret_20d'] = c.pct_change(20)*100
    df['vol_ratio_5d'] = v/v.rolling(5).mean(); df['vol_ratio_20d'] = v/v.rolling(20).mean()
    df['avg_vol_20d'] = v.rolling(20).mean()
    df['rsi_7'] = calc_rsi(c,7); df['rsi_14'] = calc_rsi(c,14)
    _,_,_,bw,bp = calc_bollinger(c); df['bb_width']=bw; df['bb_position']=bp
    df['atr_14'] = calc_atr(h,l,c); df['atr_pct'] = df['atr_14']/c*100
    m,s,mh = calc_macd(c); df['macd']=m; df['macd_hist']=mh
    df['sma_5']=c.rolling(5).mean(); df['sma_20']=c.rolling(20).mean(); df['sma_50']=c.rolling(50).mean()
    df['dist_sma20'] = (c/df['sma_20']-1)*100
    df['volatility_20d'] = c.pct_change().rolling(20).std()*100
    df['intraday_range'] = (h-l)/l*100; df['gap_pct'] = (o/c.shift(1)-1)*100
    df['high_20d']=h.rolling(20).max(); df['low_20d']=l.rolling(20).min()
    df['dist_from_20d_high'] = (c/df['high_20d']-1)*100
    return df


# ═══════════════════════════════════════════════════════
#  코드 실행 엔진
# ═══════════════════════════════════════════════════════

def execute_code(code_str, cycle_num):
    ns = {
        'pd': pd, 'np': np, 'yf': yf, 'json': json, 'random': random,
        'get_all_tickers': get_all_tickers, 'download_prices': download_prices,
        'sample_tickers': sample_tickers,
        'calc_rsi': calc_rsi, 'calc_bollinger': calc_bollinger,
        'calc_atr': calc_atr, 'calc_macd': calc_macd,
        'add_all_indicators': add_all_indicators,
        'DATASET_DIR': DATASET_DIR, 'OUTPUT_DIR': OUTPUT_DIR,
        'cycle_num': cycle_num, 'log': log,
        'datetime': datetime, 'timedelta': timedelta,
        'Counter': Counter, 'knowledge': knowledge,
        'RESULTS': {},
    }
    try:
        from sklearn.tree import DecisionTreeClassifier, export_text
        from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import cross_val_score
        ns.update({
            'DecisionTreeClassifier': DecisionTreeClassifier,
            'RandomForestClassifier': RandomForestClassifier,
            'GradientBoostingClassifier': GradientBoostingClassifier,
            'KMeans': KMeans, 'StandardScaler': StandardScaler,
            'cross_val_score': cross_val_score, 'export_text': export_text,
        })
    except ImportError: pass

    try:
        exec(code_str, ns)
        return {'success': True, 'results': ns.get('RESULTS', {}), 'error': None}
    except Exception as e:
        return {'success': False, 'results': {}, 'error': f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}"}


# ═══════════════════════════════════════════════════════
#  Phase 시스템
# ═══════════════════════════════════════════════════════

def get_phase(cycle, max_cycles):
    """
    Phase 1 — 발산 (Diverge): 넓고 다양한 탐색
    Phase 2 — 수렴 (Converge): 유망한 방향 검증
    Phase 3 — 심화 (Deepen): 전략화 + 최적화
    """
    ratio = cycle / max_cycles
    if ratio <= 0.35:
        return "DIVERGE"
    elif ratio <= 0.7:
        return "CONVERGE"
    else:
        return "DEEPEN"


SYSTEM_PROMPT = """You are a team of autonomous quant researchers inside one mind.
You think in MULTIPLE directions simultaneously, challenge your own assumptions,
and build a growing knowledge base of market patterns.

## Your Execution Environment
You have these Python tools (already imported):
- get_all_tickers() → ~10000 US tickers
- sample_tickers(n=500) → random sample for broad scans
- download_prices(tickers, start, end) → {ticker: OHLCV DataFrame}
- add_all_indicators(df) → adds RSI, BB, MACD, ATR, SMA, volatility, etc.
- calc_rsi, calc_bollinger, calc_atr, calc_macd
- pd, np, json, random, sklearn (DecisionTree, RandomForest, GradientBoosting, KMeans)
- DATASET_DIR (Path), knowledge (dict), RESULTS (dict)

## Rules
1. Write Python code inside <code> tags
2. Store ALL findings in RESULTS dict
3. Be efficient — use sample_tickers() for broad scans
4. Date range: 2020-04-01 to 2025-04-01
5. ALWAYS simulate realistic execution: D+1 Open entry, account for gaps
6. When you find something promising, IMMEDIATELY test if it holds out-of-sample
7. Never trust a single test — vary parameters and check robustness

## What Makes You Different
- You don't just test ONE idea per cycle — you test 2-3 PARALLEL hypotheses
- After each test, you CRITIQUE your own findings: "Why might this be wrong?"
- You maintain a DEAD ENDS list so you never waste time re-exploring failed paths
- You connect findings: "Pattern A + Pattern B together → what happens?"
- You think about PRACTICAL execution: slippage, liquidity, order types

## Your Ultimate Goal
Find ACTIONABLE trading patterns where:
- Signal conditions are observable BEFORE the move
- Win rate ≥ 55% with positive EV
- Works on liquid enough stocks to actually trade
- Robust across different time periods (not overfit to one year)
- Minimizes slippage (small targets, limit orders, liquid names)"""


PHASE_INSTRUCTIONS = {
    "DIVERGE": """## Phase: DIVERGE (발산 탐색)
You are in EXPLORATION mode. Cast a WIDE net.

In this cycle, explore AT LEAST 2-3 completely DIFFERENT angles simultaneously.
Think across these dimensions — pick ones NOT yet explored:

PRICE PATTERNS:
- What happens before stocks gap up >50%? >100%? >200%?
- Do stocks that crash >50% in a week tend to bounce? When?
- After earnings surprises, what predicts continuation vs reversal?
- Are there specific intraday patterns (open vs close) that predict next day?

VOLUME PATTERNS:
- Unusual volume without price movement — what follows?
- Volume dry-ups (declining for 10+ days) — what breaks the silence?
- Volume spikes at support levels — do they hold?

TECHNICAL REGIMES:
- RSI divergence (price falls but RSI rises) — does it predict reversals?
- Bollinger Band squeezes → which direction does the breakout go?
- Multiple timeframe alignment (daily + weekly signals agree)

STATISTICAL ANOMALIES:
- Day-of-week effects — are certain days better for entries?
- Month-of-year seasonality in different sectors
- Mean reversion speed — which stocks snap back fastest after drops?
- Serial correlation — does yesterday's move predict today?

CROSS-ASSET:
- Do VIX spikes create opportunities in specific stock types?
- Sector rotation patterns — can you predict which sector outperforms next?

IMPORTANT: Don't just test the obvious. Combine 2+ conditions.
"Penny stock + volume spike" is boring. What about "stock near 52w low + RSI divergence + sector momentum positive"?""",

    "CONVERGE": """## Phase: CONVERGE (수렴 검증)
You are in VALIDATION mode. Focus on the most promising findings so far.

For each promising pattern from previous cycles:
1. TEST on a DIFFERENT time period (out-of-sample)
2. TEST with DIFFERENT parameter values (robustness check)
3. COMBINE with other findings — does combining 2 patterns improve results?
4. Calculate REALISTIC metrics: D+1 Open entry, spreads, fill rates
5. Check for SURVIVORSHIP BIAS — would this have worked on delisted stocks?

CHALLENGE everything:
- "This 70% win rate — is it because I only tested on liquid stocks?"
- "This pattern works on 5 years — does it work on each individual year?"
- "Am I overfitting to a specific market regime (bull/bear/sideways)?"

KILL underperforming ideas. Be ruthless. Move them to dead_ends.
PROMOTE strong ideas to strategy candidates with specific parameters.""",

    "DEEPEN": """## Phase: DEEPEN (심화 최적화)
You are in STRATEGY BUILDING mode. Turn findings into tradeable strategies.

For each strategy candidate:
1. Define EXACT entry conditions (what signals, what thresholds)
2. Define EXACT exit rules (TP%, SL%, max hold days, trailing stop?)
3. Run a FULL backtest with:
   - D+1 Open entry (realistic)
   - Conservative SL priority (if both TP and SL hit same day, SL wins)
   - Position sizing considerations
   - Win rate, avg win, avg loss, EV, max drawdown, Sharpe-like ratio
4. Test EDGE CASES: What happens in March 2020 crash? 2022 bear market?
5. Estimate CAPACITY: How many signals per month? Is there enough liquidity?

OUTPUT a complete strategy spec that can be coded into a live scanner.
Include the specific code/logic for signal generation.""",
}


# ═══════════════════════════════════════════════════════
#  연구 사이클 실행
# ═══════════════════════════════════════════════════════

def run_cycle(cycle_num, research_log):
    phase = get_phase(cycle_num, MAX_CYCLES)
    log.info(f"\n{'='*60}")
    log.info(f"  사이클 {cycle_num}/{MAX_CYCLES} | Phase: {phase}")
    log.info(f"{'='*60}")

    # 지식 요약 구성
    knowledge_summary = {
        "confirmed_facts": knowledge["facts"][-15:],
        "active_hypotheses": knowledge["hypotheses"][-10:],
        "dead_ends": [d["text"] for d in knowledge["dead_ends"][-10:]],
        "strategy_candidates": knowledge["strategies"][-5:],
        "open_questions": knowledge["open_questions"][-10:],
    }

    prev_cycles = []
    for e in research_log[-4:]:  # 최근 4사이클만
        prev_cycles.append({
            "cycle": e["cycle"],
            "phase": e.get("phase"),
            "hypotheses_tested": e.get("hypotheses_tested", []),
            "key_findings": e.get("key_result", "N/A")[:400],
            "critique": e.get("self_critique", ""),
        })

    user_msg = f"""Cycle {cycle_num}/{MAX_CYCLES} | Phase: {phase}

{PHASE_INSTRUCTIONS[phase]}

## Current Knowledge Base
```json
{json.dumps(knowledge_summary, indent=2, default=str, ensure_ascii=False)[:4000]}
```

## Recent Cycles
```json
{json.dumps(prev_cycles, indent=2, ensure_ascii=False)[:2000]}
```

## Instructions for this cycle
1. State 2-3 HYPOTHESES you will test (label them H1, H2, H3)
2. Write ONE Python code block that tests ALL of them efficiently
3. After describing your code, write a SELF-CRITIQUE section:
   - "Why might each hypothesis be wrong?"
   - "What am I NOT seeing?"
4. Store results in RESULTS dict with keys like 'H1_result', 'H2_result', etc.
5. Also store: RESULTS['new_facts'] = [...], RESULTS['dead_ends'] = [...],
   RESULTS['new_questions'] = [...], RESULTS['strategy_candidates'] = [...]

These will be automatically merged into the knowledge base."""

    # Claude API 호출
    log.info(f"  Claude API 호출 중...")
    try:
        response = client.messages.create(
            model=MODEL, max_tokens=6000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        reply = response.content[0].text
        log.info(f"  응답 수신 ({len(reply)} chars, {response.usage.input_tokens}+{response.usage.output_tokens} tokens)")
    except Exception as e:
        log.error(f"  API 실패: {e}")
        return None, str(e)

    # 가설 추출
    hypotheses = []
    for line in reply.split('\n'):
        for prefix in ['H1', 'H2', 'H3', '**H1', '**H2', '**H3']:
            if line.strip().startswith(prefix):
                hypotheses.append(line.strip()[:200])
                break
    if not hypotheses:
        hypotheses = ["(가설 파싱 실패 — 전체 응답 참조)"]

    log.info(f"  가설: {len(hypotheses)}개")
    for h in hypotheses:
        log.info(f"    → {h[:100]}")

    # 코드 추출 + 실행
    code = None
    if '<code>' in reply and '</code>' in reply:
        code = reply.split('<code>')[1].split('</code>')[0].strip()
    elif '```python' in reply:
        code = reply.split('```python')[1].split('```')[0].strip()
    elif '```' in reply:
        parts = reply.split('```')
        if len(parts) >= 3:
            code = parts[1].strip()
            if code.startswith('python\n'): code = code[7:]

    result = None
    if code:
        log.info(f"  코드 실행 중 ({len(code)} chars)...")
        result = execute_code(code, cycle_num)
        if result['success']:
            log.info(f"  성공 — 결과 키: {list(result['results'].keys())}")
            # 지식 베이스 업데이트
            r = result['results']
            if 'new_facts' in r:
                for f in r['new_facts']:
                    knowledge['facts'].append({"id": f"F{len(knowledge['facts'])+1}",
                                                "text": str(f), "source": f"cycle{cycle_num}"})
            if 'dead_ends' in r:
                for d in r['dead_ends']:
                    knowledge['dead_ends'].append({"text": str(d), "source": f"cycle{cycle_num}"})
            if 'new_questions' in r:
                for q in r['new_questions']:
                    knowledge['open_questions'].append(str(q))
            if 'strategy_candidates' in r:
                for s in r['strategy_candidates']:
                    knowledge['strategies'].append(s)
        else:
            log.warning(f"  실패: {result['error'][:300]}")
    else:
        log.warning("  실행 가능한 코드 없음")

    # 자기 비판 추출
    critique = ""
    for marker in ["SELF-CRITIQUE", "Self-Critique", "self_critique", "CRITIQUE", "Why might"]:
        if marker in reply:
            idx = reply.index(marker)
            critique = reply[idx:idx+500]
            break

    entry = {
        "cycle": cycle_num,
        "phase": phase,
        "hypotheses_tested": hypotheses,
        "response": reply,
        "code": code,
        "result": result,
        "self_critique": critique[:300],
        "key_result": str(result['results'])[:600] if result and result['success'] else (result['error'][:300] if result else "No code"),
    }
    return entry, None


# ═══════════════════════════════════════════════════════
#  최종 리포트 생성
# ═══════════════════════════════════════════════════════

def generate_final_report(research_log):
    log.info("\n최종 리포트 생성 중...")

    log_summary = []
    for e in research_log:
        log_summary.append({
            "cycle": e["cycle"], "phase": e.get("phase"),
            "hypotheses": e.get("hypotheses_tested", []),
            "success": e["result"]["success"] if e.get("result") else False,
            "key_result": e.get("key_result", "")[:300],
            "critique": e.get("self_critique", "")[:200],
        })

    knowledge_str = json.dumps(knowledge, indent=2, default=str, ensure_ascii=False)
    if len(knowledge_str) > 6000:
        knowledge_str = knowledge_str[:6000] + "\n...(truncated)"

    prompt = f"""You are writing the FINAL RESEARCH REPORT based on {len(research_log)} cycles of autonomous research.

## Research Log
```json
{json.dumps(log_summary, indent=2, ensure_ascii=False)[:5000]}
```

## Accumulated Knowledge
```json
{knowledge_str}
```

Write a comprehensive report IN KOREAN (한국어). Structure:

# 자율 연구 에이전트 리포트

## 1. 연구 개요
- 총 사이클 수, 탐색한 가설 수, Phase별 진행 요약

## 2. 핵심 발견사항 (Top 5)
- 가장 중요한 발견들 — 구체적 수치와 함께
- 각 발견의 신뢰도 (높음/중간/낮음)

## 3. 발견된 전략 후보
For each strategy:
- 이름과 한줄 설명
- 진입 조건 (구체적)
- 청산 조건 (TP/SL/보유기간)
- 백테스트 결과 (승률, 평균수익, EV)
- 슬리피지 위험도
- 실전 적용 가능성

## 4. 폐기된 방향 (Dead Ends)
- 시도했지만 작동하지 않은 것들과 그 이유

## 5. 미해결 질문
- 추가 연구가 필요한 방향

## 6. 실전 적용 제안
- 현재 surge-scanner에 추가할 수 있는 전략이 있다면 구체적 구현 방안
- 포지션 사이징, 리스크 관리 제안

## 7. 한계점
- 과적합 위험, 생존자 편향, 데이터 한계 등

Be BRUTALLY HONEST. If nothing strong was found, say so clearly.
Numbers and specifics matter more than vague conclusions."""

    try:
        response = client.messages.create(
            model=MODEL, max_tokens=8000,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        log.error(f"리포트 생성 실패: {e}")
        return f"# 리포트 생성 실패\nError: {e}\n\n## Raw Knowledge\n```json\n{json.dumps(knowledge, indent=2, default=str)[:3000]}\n```"


# ═══════════════════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════════════════

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("  자율 주식 연구 에이전트 v2")
    log.info(f"  모델: {MODEL} | 사이클: {MAX_CYCLES}")
    log.info(f"  Phase: DIVERGE(~35%) → CONVERGE(~35%) → DEEPEN(~30%)")
    log.info("=" * 60)

    research_log = []

    for cycle in range(1, MAX_CYCLES + 1):
        entry, error = run_cycle(cycle, research_log)

        if error:
            log.error(f"사이클 {cycle} 실패: {error}")
            research_log.append({
                "cycle": cycle, "phase": get_phase(cycle, MAX_CYCLES),
                "hypotheses_tested": [], "result": {"success": False, "results": {}, "error": error},
                "key_result": error[:200],
            })
            if 'rate' in error.lower() or 'overloaded' in error.lower():
                log.info("  60초 대기 후 재시도...")
                time.sleep(60)
            continue

        research_log.append(entry)

        # 중간 저장 (매 사이클)
        with open(OUTPUT_DIR / "research_log.json", 'w') as f:
            json.dump(research_log, f, indent=2, default=str, ensure_ascii=False)
        with open(OUTPUT_DIR / "research_knowledge.json", 'w') as f:
            json.dump(knowledge, f, indent=2, default=str, ensure_ascii=False)

        log.info(f"  누적: facts={len(knowledge['facts'])} | dead_ends={len(knowledge['dead_ends'])} | strategies={len(knowledge['strategies'])} | questions={len(knowledge['open_questions'])}")

        if cycle < MAX_CYCLES:
            time.sleep(5)

    # 전략 후보 저장
    with open(OUTPUT_DIR / "research_strategies.json", 'w') as f:
        json.dump(knowledge["strategies"], f, indent=2, default=str, ensure_ascii=False)

    # 최종 리포트
    report = generate_final_report(research_log)
    with open(OUTPUT_DIR / "research_report.md", 'w') as f:
        f.write(report)

    # 최종 저장
    with open(OUTPUT_DIR / "research_log.json", 'w') as f:
        json.dump(research_log, f, indent=2, default=str, ensure_ascii=False)
    with open(OUTPUT_DIR / "research_knowledge.json", 'w') as f:
        json.dump(knowledge, f, indent=2, default=str, ensure_ascii=False)

    log.info("\n" + "=" * 60)
    log.info("  연구 완료!")
    log.info(f"  사이클: {len(research_log)} | 발견: {len(knowledge['facts'])} | 전략: {len(knowledge['strategies'])}")
    log.info(f"  리포트: {OUTPUT_DIR / 'research_report.md'}")
    log.info("=" * 60)

    print("\n" + report[:4000])
    if len(report) > 4000:
        print(f"\n... 전문: {OUTPUT_DIR / 'research_report.md'}")


if __name__ == "__main__":
    main()
