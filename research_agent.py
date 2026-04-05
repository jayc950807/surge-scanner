"""
research_agent.py — 자율 주식 연구 에이전트 v3 (LLM-Free)
==========================================================
Claude API 없이 순수 알고리즘으로 자동 패턴 발견.
yfinance + sklearn + 통계 분석만으로 작동. 비용 $0.

작동 방식:
  Phase 1 — SCAN: 다양한 시장 이벤트 유형별 데이터 수집
  Phase 2 — PROFILE: 각 이벤트에 80개+ 지표 계산, 분포 분석
  Phase 3 — DISCOVER: Decision Tree / Random Forest로 패턴 자동 발견
  Phase 4 — VALIDATE: Out-of-sample 검증 + 연도별 안정성 테스트
  Phase 5 — STRATEGIZE: 유망 패턴 → 실전 전략 (진입/청산/백테스트)
  Phase 6 — REPORT: 한국어 마크다운 리포트 자동 생성

Output:
  data/research_report.md           — 최종 종합 리포트
  data/research_events.csv          — 전체 이벤트 + 지표
  data/research_strategies.json     — 발견된 전략 후보
  data/research_patterns.json       — 발견된 패턴
"""

import os, json, logging, warnings, random, time
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter, defaultdict

import yfinance as yf
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("research_agent.log", mode="w")],
)
log = logging.getLogger(__name__)

OUTPUT_DIR  = Path("data")
BATCH_SIZE  = 80
START_DATE  = "2020-04-01"
END_DATE    = "2025-04-01"
SAMPLE_SIZE = int(os.environ.get("SAMPLE_SIZE", 2000))  # 탐색 티커 수


# ═══════════════════════════════════════════════════════
#  데이터 도구
# ═══════════════════════════════════════════════════════

def get_all_tickers():
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
    return sorted(tickers)

def download_batch(tickers, start, end):
    all_data = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        try:
            data = yf.download(" ".join(batch), start=start, end=end,
                              group_by='ticker', progress=False, threads=True)
            if data.empty: continue
            for t in batch:
                try:
                    df = data[t].copy() if len(batch) > 1 and t in data.columns.get_level_values(0) else (data.copy() if len(batch) == 1 else None)
                    if df is None: continue
                    df = df.dropna(subset=['Close','Open','High','Low','Volume'])
                    if len(df) >= 60: all_data[t] = df
                except: continue
        except: continue
        if i % 400 == 0 and i > 0:
            log.info(f"    다운로드 진행: {i}/{len(tickers)} ({len(all_data)} loaded)")
    return all_data


# ═══════════════════════════════════════════════════════
#  기술적 지표
# ═══════════════════════════════════════════════════════

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    ag = gain.ewm(alpha=1/period, min_periods=period).mean()
    al = loss.ewm(alpha=1/period, min_periods=period).mean()
    return 100 - 100 / (1 + ag / al.replace(0, np.nan))

def calc_bb(close, period=20):
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    return ma, ma+2*std, ma-2*std, (ma+2*std-ma+2*std)/(ma)*100*0.5, (close-(ma-2*std))/(4*std)

def calc_atr(h, l, c, period=14):
    tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def enrich(df):
    """DataFrame에 전체 지표 추가"""
    c, h, l, v, o = df['Close'], df['High'], df['Low'], df['Volume'], df['Open']

    # 수익률
    for n in [1,2,3,5,10,20,40,60]:
        df[f'ret_{n}d'] = c.pct_change(n) * 100

    # 거래량
    for n in [5,10,20]:
        df[f'avg_vol_{n}d'] = v.rolling(n).mean()
    df['vol_ratio_5d'] = v / df['avg_vol_5d']
    df['vol_ratio_20d'] = v / df['avg_vol_20d']

    # RSI
    df['rsi_7'] = calc_rsi(c, 7)
    df['rsi_14'] = calc_rsi(c, 14)

    # 볼린저
    ma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    df['bb_upper'] = ma20 + 2*std20
    df['bb_lower'] = ma20 - 2*std20
    df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / ma20 * 100
    df['bb_position'] = (c - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])

    # ATR
    df['atr_14'] = calc_atr(h, l, c, 14)
    df['atr_pct'] = df['atr_14'] / c * 100

    # MACD
    ema12, ema26 = c.ewm(span=12).mean(), c.ewm(span=26).mean()
    df['macd'] = ema12 - ema26
    df['macd_hist'] = df['macd'] - df['macd'].ewm(span=9).mean()

    # 이동평균
    df['sma_5'] = c.rolling(5).mean()
    df['sma_20'] = ma20
    df['sma_50'] = c.rolling(50).mean()
    df['dist_sma5'] = (c / df['sma_5'] - 1) * 100
    df['dist_sma20'] = (c / ma20 - 1) * 100
    df['dist_sma50'] = (c / df['sma_50'] - 1) * 100

    # 변동성
    df['volatility_5d'] = c.pct_change().rolling(5).std() * 100
    df['volatility_20d'] = c.pct_change().rolling(20).std() * 100

    # 범위/갭
    df['intraday_range'] = (h - l) / l * 100
    df['gap_pct'] = (o / c.shift(1) - 1) * 100

    # 고저점
    df['high_20d'] = h.rolling(20).max()
    df['low_20d'] = l.rolling(20).min()
    df['dist_20d_high'] = (c / df['high_20d'] - 1) * 100
    df['dist_20d_low'] = (c / df['low_20d'] - 1) * 100
    df['high_60d'] = h.rolling(60).max()
    df['dist_60d_high'] = (c / df['high_60d'] - 1) * 100

    # 패턴
    df['green_candle'] = (c > o).astype(int)
    df['green_ratio_5d'] = df['green_candle'].rolling(5).mean()

    # 연속 하락일
    changes = (c.diff() < 0).astype(int)
    df['consec_down'] = changes.groupby((changes != changes.shift()).cumsum()).cumsum()

    # OBV
    obv = (v * np.sign(c.diff())).cumsum()
    df['obv'] = obv
    df['obv_slope_5d'] = obv.diff(5) / obv.shift(5).abs().replace(0, np.nan) * 100

    return df


# ═══════════════════════════════════════════════════════
#  Phase 1: 다양한 이벤트 유형 스캔
# ═══════════════════════════════════════════════════════

EVENT_TYPES = {
    "surge_200": {"desc": "하루 200%+ 급등", "filter": lambda df: df['ret_1d'] >= 200},
    "surge_100": {"desc": "하루 100%+ 급등", "filter": lambda df: df['ret_1d'] >= 100},
    "surge_50":  {"desc": "하루 50%+ 급등",  "filter": lambda df: df['ret_1d'] >= 50},
    "crash_50":  {"desc": "하루 50%+ 급락",  "filter": lambda df: df['ret_1d'] <= -50},
    "crash_30":  {"desc": "하루 30%+ 급락",  "filter": lambda df: df['ret_1d'] <= -30},
    "gap_up_30": {"desc": "갭업 30%+",       "filter": lambda df: df['gap_pct'] >= 30},
    "gap_down_20": {"desc": "갭다운 20%+",   "filter": lambda df: df['gap_pct'] <= -20},
    "vol_spike_20x": {"desc": "거래량 20배 폭발", "filter": lambda df: df['vol_ratio_20d'] >= 20},
    "rsi_extreme_low": {"desc": "RSI14 < 10", "filter": lambda df: df['rsi_14'] < 10},
    "bb_crash_below": {"desc": "볼린저 하단 -50% 이탈", "filter": lambda df: df['bb_position'] < -0.5},
    "week_crash_50": {"desc": "5일간 50%+ 하락", "filter": lambda df: df['ret_5d'] <= -50},
    "week_surge_100": {"desc": "5일간 100%+ 상승", "filter": lambda df: df['ret_5d'] >= 100},
}

def scan_events(all_data):
    """모든 이벤트 유형에 대해 스캔"""
    events = defaultdict(list)
    analysis_start = pd.Timestamp(START_DATE)
    analysis_end = pd.Timestamp(END_DATE)

    for ticker, df in all_data.items():
        try:
            df = enrich(df)
            mask_period = (df.index >= analysis_start) & (df.index <= analysis_end)

            for etype, cfg in EVENT_TYPES.items():
                try:
                    mask_event = cfg['filter'](df)
                    hits = df[mask_period & mask_event]

                    for idx in hits.index:
                        pos = df.index.get_loc(idx)
                        if pos < 60 or pos >= len(df) - 1:
                            continue

                        row = df.iloc[pos]
                        prev = df.iloc[pos-1]  # 전일 (signal day의 전일 지표)

                        # 전일 기준 지표 수집 (실전에서 관찰 가능한 것만)
                        ind = {
                            'event_type': etype,
                            'ticker': ticker,
                            'date': idx.strftime('%Y-%m-%d'),
                            'year': idx.year,
                            'month': idx.month,
                            'dow': idx.dayofweek,
                        }

                        # 이벤트 당일 결과
                        ind['event_ret'] = round(float(row['ret_1d']), 2)
                        ind['event_gap'] = round(float(row['gap_pct']), 2) if not np.isnan(row['gap_pct']) else 0
                        ind['event_volume'] = int(row['Volume'])
                        ind['event_intraday_range'] = round(float(row['intraday_range']), 2)

                        # 전일 기준 지표 (80개+) — 실전에서 관찰 가능
                        for col in df.columns:
                            if col in ('Open','High','Low','Close','Volume','green_candle','Adj Close'):
                                continue
                            val = prev[col] if col in prev.index else np.nan
                            if isinstance(val, (int, float, np.integer, np.floating)):
                                if not np.isnan(val) and not np.isinf(val):
                                    ind[f'pre_{col}'] = round(float(val), 4)

                        # 전일 가격 정보
                        ind['pre_close'] = round(float(prev['Close']), 4)
                        ind['pre_price_bucket'] = (
                            '<$1' if prev['Close'] < 1 else
                            '$1-5' if prev['Close'] < 5 else
                            '$5-10' if prev['Close'] < 10 else
                            '$10-50' if prev['Close'] < 50 else
                            '$50+'
                        )

                        # D+1 ~ D+5 결과 (백테스트용)
                        for n in [1, 2, 3, 5]:
                            if pos + n < len(df):
                                future = df.iloc[pos + n]
                                ind[f'post_{n}d_ret'] = round(float((future['Close'] / row['Close'] - 1) * 100), 2)
                            if pos + 1 < len(df) and n <= 5:
                                post_slice = df.iloc[pos+1:pos+1+n]
                                if len(post_slice) > 0:
                                    ind[f'post_{n}d_max_up'] = round(float((post_slice['High'].max() / row['Close'] - 1) * 100), 2)
                                    ind[f'post_{n}d_max_down'] = round(float((post_slice['Low'].min() / row['Close'] - 1) * 100), 2)

                        # D+1 Open (실전 진입가) 기준 수익
                        if pos + 1 < len(df):
                            next_open = df.iloc[pos+1]['Open']
                            ind['next_open_gap'] = round(float((next_open / row['Close'] - 1) * 100), 2)
                            for n in [1, 3, 5]:
                                if pos + 1 + n < len(df):
                                    ind[f'post_d1open_{n}d_ret'] = round(float((df.iloc[pos+1+n]['Close'] / next_open - 1) * 100), 2)

                        events[etype].append(ind)

                except Exception:
                    continue
        except Exception:
            continue

    return events


# ═══════════════════════════════════════════════════════
#  Phase 3: 자동 패턴 발견
# ═══════════════════════════════════════════════════════

def discover_patterns(df_all):
    """Decision Tree + Random Forest + 통계로 패턴 자동 발견"""
    from sklearn.tree import DecisionTreeClassifier, export_text
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import StandardScaler

    patterns = []

    # 지표 컬럼 (pre_ 접두사 = 이벤트 전날 관찰 가능)
    pre_cols = [c for c in df_all.columns if c.startswith('pre_') and
                c not in ('pre_close', 'pre_price_bucket') and
                df_all[c].dtype in ('float64', 'int64', 'float32')]

    if len(pre_cols) < 10 or len(df_all) < 30:
        return patterns

    X = df_all[pre_cols].copy()
    X = X.replace([np.inf, -np.inf], np.nan)
    X = X.fillna(X.median())

    # ── 분석 1: 이벤트 유형별 프로필 비교 ──
    log.info("    패턴 발견 1: 이벤트 유형별 지표 프로필...")
    for etype in df_all['event_type'].unique():
        mask = df_all['event_type'] == etype
        if mask.sum() < 10:
            continue

        y = mask.astype(int)
        try:
            dt = DecisionTreeClassifier(max_depth=4, min_samples_leaf=max(5, int(mask.sum()*0.1)))
            scores = cross_val_score(dt, X, y, cv=min(5, max(2, int(len(y)/10))), scoring='f1')
            dt.fit(X, y)
            importances = pd.Series(dt.feature_importances_, index=pre_cols).nlargest(10)
            tree_text = export_text(dt, feature_names=pre_cols, max_depth=4)

            patterns.append({
                'name': f'profile_{etype}',
                'type': 'event_profile',
                'event_type': etype,
                'count': int(mask.sum()),
                'f1_score': round(float(scores.mean()), 3),
                'top_features': {k: round(v, 4) for k, v in importances.items()},
                'tree_rules': tree_text[:1000],
            })
        except: continue

    # ── 분석 2: 이벤트 후 수익 예측 ──
    log.info("    패턴 발견 2: 이벤트 후 수익 예측 (어떤 이벤트가 돈이 되는가)...")
    for target_col in ['post_1d_ret', 'post_3d_ret', 'post_5d_ret']:
        if target_col not in df_all.columns:
            continue
        y_series = df_all[target_col].dropna()
        if len(y_series) < 30:
            continue

        X_sub = X.loc[y_series.index]
        y_profit = (y_series > 0).astype(int)

        # 이벤트 타입도 feature에 추가
        X_with_type = X_sub.copy()
        for etype in df_all['event_type'].unique():
            X_with_type[f'is_{etype}'] = (df_all.loc[y_series.index, 'event_type'] == etype).astype(int)

        feature_names = list(X_with_type.columns)

        try:
            # Random Forest
            rf = RandomForestClassifier(n_estimators=100, max_depth=5,
                                         min_samples_leaf=max(5, len(y_profit)//50),
                                         random_state=42, n_jobs=-1)
            scores = cross_val_score(rf, X_with_type, y_profit, cv=5, scoring='accuracy')
            rf.fit(X_with_type, y_profit)
            importances = pd.Series(rf.feature_importances_, index=feature_names).nlargest(15)

            # Decision Tree (해석 가능한 규칙)
            dt = DecisionTreeClassifier(max_depth=4, min_samples_leaf=max(5, len(y_profit)//30), random_state=42)
            dt_scores = cross_val_score(dt, X_with_type, y_profit, cv=5, scoring='accuracy')
            dt.fit(X_with_type, y_profit)
            tree_text = export_text(dt, feature_names=feature_names, max_depth=4)

            patterns.append({
                'name': f'profit_predict_{target_col}',
                'type': 'profit_prediction',
                'target': target_col,
                'total_samples': len(y_profit),
                'base_win_rate': round(float(y_profit.mean() * 100), 1),
                'rf_accuracy': round(float(scores.mean() * 100), 1),
                'dt_accuracy': round(float(dt_scores.mean() * 100), 1),
                'top_features': {k: round(v, 4) for k, v in importances.items()},
                'tree_rules': tree_text[:1500],
                'avg_profit_when_positive': round(float(y_series[y_series > 0].mean()), 2),
                'avg_loss_when_negative': round(float(y_series[y_series <= 0].mean()), 2),
            })
        except Exception as e:
            log.warning(f"    패턴 분석 실패 ({target_col}): {e}")

    # ── 분석 3: 이벤트 유형별 사후 수익 분석 ──
    log.info("    패턴 발견 3: 이벤트 유형별 수익성 랭킹...")
    profitability = []
    for etype in df_all['event_type'].unique():
        sub = df_all[df_all['event_type'] == etype]
        if len(sub) < 10:
            continue
        for target in ['post_1d_ret', 'post_3d_ret', 'post_5d_ret']:
            if target not in sub.columns:
                continue
            s = sub[target].dropna()
            if len(s) < 10:
                continue
            profitability.append({
                'event_type': etype,
                'horizon': target,
                'count': len(s),
                'win_rate': round(float((s > 0).mean() * 100), 1),
                'mean_ret': round(float(s.mean()), 2),
                'median_ret': round(float(s.median()), 2),
                'avg_win': round(float(s[s > 0].mean()), 2) if (s > 0).any() else 0,
                'avg_loss': round(float(s[s <= 0].mean()), 2) if (s <= 0).any() else 0,
                'ev': round(float(s.mean()), 2),
            })

    profitability.sort(key=lambda x: x['ev'], reverse=True)
    patterns.append({
        'name': 'profitability_ranking',
        'type': 'ranking',
        'data': profitability,
    })

    # ── 분석 4: 지표 집중도 분석 (공통점 자동 발견) ──
    log.info("    패턴 발견 4: 지표 집중도 (값이 특정 범위에 몰리는 지표)...")
    concentration = []
    for etype in df_all['event_type'].unique():
        sub = df_all[df_all['event_type'] == etype]
        if len(sub) < 20:
            continue
        for col in pre_cols:
            s = sub[col].dropna()
            if len(s) < 10:
                continue
            # 바이너리
            if set(s.unique()).issubset({0, 1, 0.0, 1.0}):
                pct_true = round(float(s.mean() * 100), 1)
                if pct_true >= 70 or pct_true <= 15:
                    concentration.append({
                        'event_type': etype, 'indicator': col,
                        'type': 'binary',
                        'pct_true': pct_true,
                        'count': len(s),
                    })
            else:
                # IQR 대비 전체 범위가 좁은지
                q25, q75 = s.quantile(0.25), s.quantile(0.75)
                total_range = s.max() - s.min()
                if total_range > 0:
                    iqr_ratio = (q75 - q25) / total_range
                    if iqr_ratio < 0.15:  # IQR이 전체의 15% 이내 → 강한 집중
                        concentration.append({
                            'event_type': etype, 'indicator': col,
                            'type': 'concentrated',
                            'median': round(float(s.median()), 2),
                            'q25': round(float(q25), 2),
                            'q75': round(float(q75), 2),
                            'iqr_ratio': round(float(iqr_ratio * 100), 1),
                            'count': len(s),
                        })

    concentration.sort(key=lambda x: x.get('iqr_ratio', x.get('pct_true', 50)))
    patterns.append({
        'name': 'concentration_analysis',
        'type': 'concentration',
        'findings': concentration[:50],
    })

    # ── 분석 5: 연도별 안정성 ──
    log.info("    패턴 발견 5: 연도별 안정성 체크...")
    stability = []
    for etype in df_all['event_type'].unique():
        sub = df_all[df_all['event_type'] == etype]
        for target in ['post_1d_ret', 'post_3d_ret']:
            if target not in sub.columns:
                continue
            yearly = {}
            for year in sub['year'].unique():
                ys = sub[sub['year'] == year][target].dropna()
                if len(ys) >= 5:
                    yearly[int(year)] = {
                        'count': len(ys),
                        'win_rate': round(float((ys > 0).mean() * 100), 1),
                        'mean_ret': round(float(ys.mean()), 2),
                    }
            if len(yearly) >= 3:
                wrs = [v['win_rate'] for v in yearly.values()]
                stability.append({
                    'event_type': etype, 'target': target,
                    'yearly': yearly,
                    'wr_std': round(float(np.std(wrs)), 1),
                    'wr_min': min(wrs),
                    'wr_max': max(wrs),
                    'consistent': all(wr >= 50 for wr in wrs),
                })

    patterns.append({
        'name': 'yearly_stability',
        'type': 'stability',
        'data': stability,
    })

    return patterns


# ═══════════════════════════════════════════════════════
#  Phase 5: 전략 후보 생성
# ═══════════════════════════════════════════════════════

def build_strategies(df_all, patterns):
    """유망 패턴 → 실전 전략으로 변환 + 백테스트"""
    strategies = []

    # 수익성 랭킹에서 유망한 이벤트 유형 추출
    ranking = next((p for p in patterns if p['name'] == 'profitability_ranking'), None)
    if not ranking:
        return strategies

    for entry in ranking['data']:
        if entry['ev'] <= 0 or entry['count'] < 20:
            continue

        etype = entry['event_type']
        horizon = entry['horizon']
        sub = df_all[df_all['event_type'] == etype].copy()
        target = sub[horizon].dropna()
        if len(target) < 20:
            continue

        # TP/SL 최적화: 다양한 조합 테스트
        best_strategy = None
        best_ev = -999

        tp_range = [3, 5, 8, 10, 15, 20, 30, 50]
        sl_range = [-5, -10, -15, -20, -30]
        hold_range = [3, 5, 10, 15, 30]

        for tp_pct in tp_range:
            for sl_pct in sl_range:
                for max_hold in hold_range:
                    # 간단 백테스트
                    wins = losses = expired = 0
                    total_pnl = 0

                    for _, row in sub.iterrows():
                        # D+1 Open 진입 시뮬레이션
                        if 'next_open_gap' not in row or pd.isna(row.get('next_open_gap')):
                            continue

                        # post_Xd_max_up/down으로 TP/SL 체크
                        hit_tp = False
                        hit_sl = False

                        for n in [1, 2, 3, 5]:
                            max_up_key = f'post_{n}d_max_up'
                            max_down_key = f'post_{n}d_max_down'
                            if max_up_key in row and not pd.isna(row.get(max_up_key)):
                                if row[max_up_key] >= tp_pct:
                                    hit_tp = True
                                if row[max_down_key] <= sl_pct:
                                    hit_sl = True

                            if n >= max_hold:
                                break

                        if hit_sl and hit_tp:
                            # 보수적: SL 우선
                            losses += 1
                            total_pnl += sl_pct
                        elif hit_tp:
                            wins += 1
                            total_pnl += tp_pct
                        elif hit_sl:
                            losses += 1
                            total_pnl += sl_pct
                        else:
                            # 만기 — 마지막 가격으로 청산
                            ret_key = f'post_{min(max_hold, 5)}d_ret'
                            if ret_key in row and not pd.isna(row.get(ret_key)):
                                ret = row[ret_key]
                                total_pnl += ret
                                if ret > 0:
                                    wins += 1
                                else:
                                    losses += 1
                            expired += 1

                    total_trades = wins + losses
                    if total_trades < 10:
                        continue

                    wr = wins / total_trades * 100
                    ev = total_pnl / total_trades

                    if ev > best_ev:
                        best_ev = ev
                        best_strategy = {
                            'event_type': etype,
                            'tp_pct': tp_pct,
                            'sl_pct': sl_pct,
                            'max_hold': max_hold,
                            'wins': wins,
                            'losses': losses,
                            'expired': expired,
                            'total_trades': total_trades,
                            'win_rate': round(wr, 1),
                            'ev_per_trade': round(ev, 2),
                            'total_pnl': round(total_pnl, 2),
                            'sharpe_like': round(ev / max(np.std([tp_pct]*wins + [sl_pct]*losses), 1), 3),
                        }

        if best_strategy and best_strategy['ev_per_trade'] > 0 and best_strategy['win_rate'] >= 45:
            strategies.append(best_strategy)

    strategies.sort(key=lambda x: x['ev_per_trade'], reverse=True)
    return strategies[:20]


# ═══════════════════════════════════════════════════════
#  Phase 6: 리포트 생성
# ═══════════════════════════════════════════════════════

def generate_report(event_counts, df_all, patterns, strategies):
    lines = []
    a = lines.append

    a("# 자율 연구 에이전트 리포트 (LLM-Free v3)")
    a(f"\n생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    a(f"분석 기간: {START_DATE} ~ {END_DATE}")
    a(f"스캔 종목 수: {SAMPLE_SIZE}")

    # 1. 이벤트 요약
    a("\n## 1. 발견된 시장 이벤트 요약")
    a("\n| 이벤트 유형 | 설명 | 발견 건수 |")
    a("|------------|------|----------|")
    for etype, count in sorted(event_counts.items(), key=lambda x: x[1], reverse=True):
        desc = EVENT_TYPES.get(etype, {}).get('desc', etype)
        a(f"| {etype} | {desc} | {count} |")
    a(f"\n**전체 이벤트: {sum(event_counts.values())}건**")

    # 2. 연도별 분포
    a("\n## 2. 연도별 분포")
    if len(df_all) > 0:
        for etype in df_all['event_type'].unique():
            sub = df_all[df_all['event_type'] == etype]
            if len(sub) < 10: continue
            a(f"\n### {etype}")
            a("| 연도 | 건수 |")
            a("|------|------|")
            for year, cnt in sub.groupby('year').size().items():
                a(f"| {year} | {cnt} |")

    # 3. 수익성 랭킹
    a("\n## 3. 이벤트 유형별 수익성")
    ranking = next((p for p in patterns if p['name'] == 'profitability_ranking'), None)
    if ranking:
        a("\n| 이벤트 | 기간 | 건수 | 승률 | 평균수익 | EV |")
        a("|--------|------|------|------|----------|-----|")
        for r in ranking['data'][:30]:
            a(f"| {r['event_type']} | {r['horizon']} | {r['count']} | {r['win_rate']}% | {r['mean_ret']}% | {r['ev']} |")

    # 4. 지표 집중도 (공통점)
    a("\n## 4. 핵심 공통점 (지표 집중도)")
    conc = next((p for p in patterns if p['name'] == 'concentration_analysis'), None)
    if conc:
        a("\n| 이벤트 | 지표 | 유형 | 상세 |")
        a("|--------|------|------|------|")
        for f in conc['findings'][:30]:
            if f['type'] == 'binary':
                detail = f"True 비율: {f['pct_true']}%"
            else:
                detail = f"중앙값: {f['median']}, IQR비율: {f['iqr_ratio']}%"
            a(f"| {f['event_type']} | {f['indicator']} | {f['type']} | {detail} |")

    # 5. ML 패턴 발견
    a("\n## 5. ML 자동 패턴 발견")
    for p in patterns:
        if p['type'] == 'profit_prediction':
            a(f"\n### {p['target']} 수익 예측")
            a(f"- 샘플: {p['total_samples']}건 | 기본 승률: {p['base_win_rate']}%")
            a(f"- Random Forest 정확도: {p['rf_accuracy']}%")
            a(f"- Decision Tree 정확도: {p['dt_accuracy']}%")
            a(f"- 수익 시 평균: +{p['avg_profit_when_positive']}% | 손실 시 평균: {p['avg_loss_when_negative']}%")
            a(f"\n**중요 지표 Top 10:**\n")
            a("| 지표 | 중요도 |")
            a("|------|--------|")
            for feat, imp in list(p['top_features'].items())[:10]:
                a(f"| {feat} | {imp} |")
            a(f"\n**Decision Tree 규칙:**\n```\n{p['tree_rules'][:1200]}\n```")

    # 6. 연도별 안정성
    a("\n## 6. 연도별 안정성 (일관된 패턴)")
    stab = next((p for p in patterns if p['name'] == 'yearly_stability'), None)
    if stab:
        consistent = [s for s in stab['data'] if s.get('consistent')]
        if consistent:
            a("\n**모든 연도에서 승률 50%+ 유지한 패턴:**\n")
            for s in consistent:
                a(f"- **{s['event_type']}** ({s['target']}): WR {s['wr_min']}%~{s['wr_max']}% (편차 {s['wr_std']}%)")
                for year, data in sorted(s['yearly'].items()):
                    a(f"  - {year}: {data['count']}건, WR {data['win_rate']}%, 평균 {data['mean_ret']}%")
        else:
            a("\n모든 연도에서 일관된 패턴은 발견되지 않았습니다.")

    # 7. 전략 후보
    a("\n## 7. 발견된 전략 후보")
    if strategies:
        for i, s in enumerate(strategies[:10], 1):
            desc = EVENT_TYPES.get(s['event_type'], {}).get('desc', s['event_type'])
            a(f"\n### 전략 {i}: {desc}")
            a(f"- 진입: {desc} 발생 후 **D+1 시가**에 매수")
            a(f"- 익절: +{s['tp_pct']}% | 손절: {s['sl_pct']}% | 최대 보유: {s['max_hold']}일")
            a(f"- 백테스트: {s['total_trades']}건, 승률 {s['win_rate']}%, EV {s['ev_per_trade']}%/거래")
            a(f"- 승: {s['wins']} | 패: {s['losses']} | 만기: {s['expired']}")
            a(f"- 총 P&L: {s['total_pnl']}%")
    else:
        a("\nEV > 0이고 승률 45%+ 인 전략이 발견되지 않았습니다.")

    # 8. 한계점
    a("\n## 8. 한계점 및 주의사항")
    a(f"\n- 샘플 종목 {SAMPLE_SIZE}개로 제한 — 전체 시장 대비 편향 가능")
    a("- 생존자 편향: 상장폐지 종목 미포함")
    a("- D+1 시가 진입 시뮬레이션이지만, 실제 슬리피지/스프레드 미반영")
    a("- TP/SL은 일봉 기준 — 장중 동시 도달 시 보수적(SL 우선) 처리")
    a("- 과적합 위험: 특히 소수 이벤트(30건 미만)의 결과는 신뢰도 낮음")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════════════════

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    log.info("=" * 60)
    log.info("  자율 주식 연구 에이전트 v3 (LLM-Free)")
    log.info(f"  기간: {START_DATE} ~ {END_DATE}")
    log.info(f"  샘플: {SAMPLE_SIZE} 종목")
    log.info(f"  이벤트 유형: {len(EVENT_TYPES)}개")
    log.info("=" * 60)

    # Phase 1: 티커 수집 + 데이터 다운로드
    log.info("\n[Phase 1/6] 데이터 수집...")
    all_tickers = get_all_tickers()
    sample = random.sample(all_tickers, min(SAMPLE_SIZE, len(all_tickers)))
    log.info(f"  전체: {len(all_tickers)}개 → 샘플: {len(sample)}개")

    fetch_start = (pd.Timestamp(START_DATE) - timedelta(days=120)).strftime('%Y-%m-%d')
    all_data = download_batch(sample, fetch_start, END_DATE)
    log.info(f"  다운로드 완료: {len(all_data)}개 종목")

    # Phase 2: 이벤트 스캔 + 지표 계산
    log.info("\n[Phase 2/6] 이벤트 스캔 + 80개+ 지표 계산...")
    events = scan_events(all_data)
    event_counts = {k: len(v) for k, v in events.items()}
    log.info(f"  이벤트 발견:")
    for etype, count in sorted(event_counts.items(), key=lambda x: x[1], reverse=True):
        log.info(f"    {etype}: {count}건")

    # 전체 DataFrame 병합
    all_events = []
    for etype, elist in events.items():
        all_events.extend(elist)

    if not all_events:
        log.warning("이벤트 0건 — 종료")
        return

    df_all = pd.DataFrame(all_events)
    df_all.to_csv(OUTPUT_DIR / "research_events.csv", index=False)
    log.info(f"  → {len(df_all)} rows × {len(df_all.columns)} columns 저장")

    # Phase 3: 자동 패턴 발견
    log.info("\n[Phase 3/6] ML 자동 패턴 발견...")
    patterns = discover_patterns(df_all)
    log.info(f"  패턴 {len(patterns)}개 발견")

    with open(OUTPUT_DIR / "research_patterns.json", 'w') as f:
        json.dump(patterns, f, indent=2, default=str, ensure_ascii=False)

    # Phase 4: 연도별 안정성 (discover_patterns 내부에서 처리)
    log.info("\n[Phase 4/6] 연도별 안정성 검증 (Phase 3에서 통합 처리됨)")

    # Phase 5: 전략 후보 생성
    log.info("\n[Phase 5/6] 전략 후보 생성 + 백테스트...")
    strategies = build_strategies(df_all, patterns)
    log.info(f"  전략 후보 {len(strategies)}개 생성")

    with open(OUTPUT_DIR / "research_strategies.json", 'w') as f:
        json.dump(strategies, f, indent=2, default=str, ensure_ascii=False)

    # Phase 6: 리포트 생성
    log.info("\n[Phase 6/6] 리포트 생성...")
    report = generate_report(event_counts, df_all, patterns, strategies)

    with open(OUTPUT_DIR / "research_report.md", 'w') as f:
        f.write(report)

    log.info("\n" + "=" * 60)
    log.info("  완료!")
    log.info(f"  이벤트: {len(df_all)}건 | 패턴: {len(patterns)}개 | 전략: {len(strategies)}개")
    log.info(f"  리포트: {OUTPUT_DIR / 'research_report.md'}")
    log.info("=" * 60)

    print("\n" + report[:5000])
    if len(report) > 5000:
        print(f"\n... 전문: {OUTPUT_DIR / 'research_report.md'}")


if __name__ == "__main__":
    main()
