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

# 실전 필터 (페니스톡/저유동성 제외)
MIN_PRICE         = 2.0        # 전일 종가 $2 이상
MIN_DOLLAR_VOL    = 500_000    # 전일 평균 거래대금 $50만 이상
# 수익률 클리핑 (극단값 오염 방지)
RET_CLIP_UP       = 300.0      # +300% 초과는 300%로 캡
RET_CLIP_DOWN     = -95.0      # -95% 이하는 -95%로 플로어


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

                        # ── 실전 필터 (페니스톡/저유동성 제외) ──
                        if not np.isfinite(prev['Close']) or prev['Close'] < MIN_PRICE:
                            continue
                        prev_avg_vol20 = prev.get('avg_vol_20d', np.nan)
                        if np.isfinite(prev_avg_vol20):
                            prev_dollar_vol = prev_avg_vol20 * prev['Close']
                            if prev_dollar_vol < MIN_DOLLAR_VOL:
                                continue

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

                        def _clip(x):
                            return max(RET_CLIP_DOWN, min(RET_CLIP_UP, float(x)))

                        # D+1 ~ D+5 결과 (백테스트용) — D+1 Open 진입 기준으로 통일
                        if pos + 1 < len(df):
                            next_open = df.iloc[pos+1]['Open']
                            if np.isfinite(next_open) and next_open > 0:
                                ind['next_open_gap'] = round(_clip((next_open / row['Close'] - 1) * 100), 2)
                                for n in [1, 2, 3, 5]:
                                    if pos + 1 + n - 1 < len(df):
                                        end_close = df.iloc[pos + n]['Close'] if n == 1 else df.iloc[pos + n]['Close']
                                        # 실전 기준: D+1 Open 매수 → D+n Close 청산
                                        exit_idx = pos + n
                                        if exit_idx < len(df):
                                            exit_close = df.iloc[exit_idx]['Close']
                                            ind[f'post_{n}d_ret'] = round(_clip((exit_close / next_open - 1) * 100), 2)
                                    if pos + 1 + n <= len(df):
                                        # 보유 기간 중 장중 최고/최저 (D+1 Open 기준)
                                        hold_slice = df.iloc[pos+1:pos+1+n]
                                        if len(hold_slice) > 0:
                                            ind[f'post_{n}d_max_up'] = round(_clip((hold_slice['High'].max() / next_open - 1) * 100), 2)
                                            ind[f'post_{n}d_max_down'] = round(_clip((hold_slice['Low'].min() / next_open - 1) * 100), 2)

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
            # Random Forest — class_weight='balanced' 로 33% 편향 교정
            # f1_macro로 평가해 "전부 0" 베이스라인을 무력화
            rf = RandomForestClassifier(n_estimators=150, max_depth=6,
                                         min_samples_leaf=max(5, len(y_profit)//50),
                                         class_weight='balanced',
                                         random_state=42, n_jobs=-1)
            scores = cross_val_score(rf, X_with_type, y_profit, cv=5, scoring='f1_macro')
            acc_scores = cross_val_score(rf, X_with_type, y_profit, cv=5, scoring='accuracy')
            rf.fit(X_with_type, y_profit)
            importances = pd.Series(rf.feature_importances_, index=feature_names).nlargest(15)

            # Decision Tree (해석 가능한 규칙)
            dt = DecisionTreeClassifier(max_depth=4,
                                         min_samples_leaf=max(5, len(y_profit)//30),
                                         class_weight='balanced',
                                         random_state=42)
            dt_scores = cross_val_score(dt, X_with_type, y_profit, cv=5, scoring='f1_macro')
            dt.fit(X_with_type, y_profit)
            tree_text = export_text(dt, feature_names=feature_names, max_depth=4)

            # 중앙값/트림평균으로 극단값 오염 방지
            pos_s = y_series[y_series > 0]
            neg_s = y_series[y_series <= 0]
            patterns.append({
                'name': f'profit_predict_{target_col}',
                'type': 'profit_prediction',
                'target': target_col,
                'total_samples': len(y_profit),
                'base_win_rate': round(float(y_profit.mean() * 100), 1),
                'rf_f1_macro': round(float(scores.mean()), 3),
                'rf_accuracy': round(float(acc_scores.mean() * 100), 1),
                'dt_f1_macro': round(float(dt_scores.mean()), 3),
                'top_features': {k: round(v, 4) for k, v in importances.items()},
                'tree_rules': tree_text[:1500],
                'median_profit_when_positive': round(float(pos_s.median()), 2) if len(pos_s) > 0 else 0,
                'median_loss_when_negative': round(float(neg_s.median()), 2) if len(neg_s) > 0 else 0,
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
            # 5% 트림 평균 (상하위 5% 제거)으로 극단값 오염 방지
            lo, hi = s.quantile(0.05), s.quantile(0.95)
            trimmed = s[(s >= lo) & (s <= hi)]
            trim_mean = float(trimmed.mean()) if len(trimmed) > 0 else float(s.median())
            profitability.append({
                'event_type': etype,
                'horizon': target,
                'count': len(s),
                'win_rate': round(float((s > 0).mean() * 100), 1),
                'median_ret': round(float(s.median()), 2),
                'trim_mean_ret': round(trim_mean, 2),
                'avg_win': round(float(s[s > 0].median()), 2) if (s > 0).any() else 0,
                'avg_loss': round(float(s[s <= 0].median()), 2) if (s <= 0).any() else 0,
                'ev': round(trim_mean, 2),  # EV = 트림 평균 기준
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
                # 변동계수(CV) 기반 집중도: |median|이 0에 가까우면 IQR/median 사용
                # 극단값 영향 제거를 위해 max-min 대신 p05~p95 범위 사용
                q05, q25, q50, q75, q95 = s.quantile([0.05, 0.25, 0.5, 0.75, 0.95]).values
                robust_range = q95 - q05
                iqr = q75 - q25
                if robust_range > 1e-9:
                    iqr_ratio = iqr / robust_range
                    # 집중도: IQR이 p05~p95 범위의 40% 이내 (현실적 기준)
                    if iqr_ratio < 0.40:
                        concentration.append({
                            'event_type': etype, 'indicator': col,
                            'type': 'concentrated',
                            'median': round(float(q50), 3),
                            'q25': round(float(q25), 3),
                            'q75': round(float(q75), 3),
                            'p05': round(float(q05), 3),
                            'p95': round(float(q95), 3),
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

def _backtest_single(sub, tp_pct, sl_pct, max_hold):
    """단일 TP/SL/hold 조합 백테스트. 각 거래의 PnL 리스트와 메타 반환."""
    trades = []
    for _, row in sub.iterrows():
        if pd.isna(row.get('next_open_gap')):
            continue
        hit_tp_day = None
        hit_sl_day = None
        for n in [1, 2, 3, 5]:
            if n > max_hold:
                break
            max_up = row.get(f'post_{n}d_max_up')
            max_dn = row.get(f'post_{n}d_max_down')
            if pd.isna(max_up) or pd.isna(max_dn):
                continue
            if hit_tp_day is None and max_up >= tp_pct:
                hit_tp_day = n
            if hit_sl_day is None and max_dn <= sl_pct:
                hit_sl_day = n
        # 결정 규칙: 같은 날 동시 도달 시 SL 우선(보수적)
        if hit_sl_day is not None and hit_tp_day is not None:
            pnl = sl_pct if hit_sl_day <= hit_tp_day else tp_pct
            outcome = 'sl' if hit_sl_day <= hit_tp_day else 'tp'
        elif hit_tp_day is not None:
            pnl, outcome = tp_pct, 'tp'
        elif hit_sl_day is not None:
            pnl, outcome = sl_pct, 'sl'
        else:
            # 만기 청산: 가장 가까운 post_{n}d_ret 사용 (D+1 Open 기준이므로 정확)
            ret_key = f'post_{min(max_hold, 5)}d_ret'
            ret = row.get(ret_key)
            if pd.isna(ret):
                continue
            pnl, outcome = float(ret), 'exp'
        trades.append({'pnl': pnl, 'outcome': outcome, 'year': int(row.get('year', 0))})
    return trades


def build_strategies(df_all, patterns):
    """유망 패턴 → 실전 전략으로 변환 + 백테스트 (중복 제거 + train/test 분리)"""
    strategies = []

    ranking = next((p for p in patterns if p['name'] == 'profitability_ranking'), None)
    if not ranking:
        return strategies

    # 이벤트 타입별로 가장 좋은 horizon 1개만 선택 (중복 제거)
    seen_etypes = set()
    candidate_etypes = []
    for entry in ranking['data']:
        if entry['event_type'] in seen_etypes:
            continue
        if entry['ev'] <= 0 or entry['count'] < 30:
            continue
        seen_etypes.add(entry['event_type'])
        candidate_etypes.append(entry['event_type'])

    tp_range = [5, 8, 10, 15, 20, 30, 50]
    sl_range = [-5, -10, -15, -20, -30]
    hold_range = [3, 5, 10]

    for etype in candidate_etypes:
        sub = df_all[df_all['event_type'] == etype].copy()
        sub = sub.sort_values('date')  # 시간순 정렬
        if len(sub) < 40:
            continue

        # Train/Test 분리 (80/20, 시간순 — look-ahead bias 방지)
        split = int(len(sub) * 0.7)
        train, test = sub.iloc[:split], sub.iloc[split:]

        # Train에서 최적 TP/SL 탐색
        best = None
        best_ev = -1e9
        for tp in tp_range:
            for sl in sl_range:
                for h in hold_range:
                    tr = _backtest_single(train, tp, sl, h)
                    if len(tr) < 15:
                        continue
                    ev = np.mean([t['pnl'] for t in tr])
                    if ev > best_ev:
                        best_ev = ev
                        best = (tp, sl, h)

        if best is None:
            continue
        tp, sl, h = best

        # Test에서 out-of-sample 검증
        test_trades = _backtest_single(test, tp, sl, h)
        train_trades = _backtest_single(train, tp, sl, h)
        all_trades = _backtest_single(sub, tp, sl, h)

        if len(all_trades) < 20:
            continue

        # 연도별 분해
        yearly = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
        for t in all_trades:
            y = t['year']
            if t['pnl'] > 0:
                yearly[y]['wins'] += 1
            else:
                yearly[y]['losses'] += 1
            yearly[y]['pnl'] += t['pnl']
        yearly_out = {}
        for y, d in sorted(yearly.items()):
            n = d['wins'] + d['losses']
            yearly_out[y] = {
                'n': n,
                'win_rate': round(d['wins']/n*100, 1) if n else 0,
                'total_pnl': round(d['pnl'], 1),
                'ev': round(d['pnl']/n, 2) if n else 0,
            }

        def _summ(trades):
            if not trades:
                return None
            pnls = [t['pnl'] for t in trades]
            wins = sum(1 for p in pnls if p > 0)
            return {
                'n': len(trades),
                'win_rate': round(wins/len(trades)*100, 1),
                'ev': round(float(np.mean(pnls)), 2),
                'median_pnl': round(float(np.median(pnls)), 2),
                'total_pnl': round(float(np.sum(pnls)), 1),
                'std': round(float(np.std(pnls)), 2),
            }

        train_sum = _summ(train_trades)
        test_sum = _summ(test_trades)
        all_sum = _summ(all_trades)

        # 필터: OOS EV > 0 + 전체 승률 40%+
        if test_sum is None or test_sum['ev'] <= 0 or all_sum['win_rate'] < 40:
            continue

        strategies.append({
            'event_type': etype,
            'tp_pct': tp, 'sl_pct': sl, 'max_hold': h,
            'all': all_sum, 'train': train_sum, 'test': test_sum,
            'yearly': yearly_out,
        })

    strategies.sort(key=lambda x: x['test']['ev'], reverse=True)
    return strategies[:10]


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
    a("\n## 3. 이벤트 유형별 수익성 (트림평균 기준, 극단값 제거)")
    ranking = next((p for p in patterns if p['name'] == 'profitability_ranking'), None)
    if ranking:
        a("\n| 이벤트 | 기간 | 건수 | 승률 | 중앙값 | 트림평균(EV) |")
        a("|--------|------|------|------|--------|------|")
        for r in ranking['data'][:30]:
            a(f"| {r['event_type']} | {r['horizon']} | {r['count']} | {r['win_rate']}% | {r['median_ret']}% | {r['trim_mean_ret']}% |")

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
    a("\n## 5. ML 자동 패턴 발견 (class_weight='balanced' 적용)")
    for p in patterns:
        if p['type'] == 'profit_prediction':
            a(f"\n### {p['target']} 수익 예측")
            a(f"- 샘플: {p['total_samples']}건 | 기본 승률: {p['base_win_rate']}%")
            a(f"- Random Forest F1(macro): {p['rf_f1_macro']} | 정확도: {p['rf_accuracy']}%")
            a(f"- Decision Tree F1(macro): {p['dt_f1_macro']}")
            a(f"- 수익 시 중앙값: +{p['median_profit_when_positive']}% | 손실 시 중앙값: {p['median_loss_when_negative']}%")
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
    a("\n## 7. 발견된 전략 후보 (Train/Test 분리 + 연도별 분해)")
    if strategies:
        for i, s in enumerate(strategies, 1):
            desc = EVENT_TYPES.get(s['event_type'], {}).get('desc', s['event_type'])
            a(f"\n### 전략 {i}: {desc}")
            a(f"- 진입: {desc} 발생 후 **D+1 시가**에 매수")
            a(f"- 익절: +{s['tp_pct']}% | 손절: {s['sl_pct']}% | 최대 보유: {s['max_hold']}일")
            a(f"- **전체**: {s['all']['n']}건, 승률 {s['all']['win_rate']}%, EV {s['all']['ev']}%/거래, 총 {s['all']['total_pnl']}%")
            a(f"- **Train(70%)**: {s['train']['n']}건, 승률 {s['train']['win_rate']}%, EV {s['train']['ev']}%")
            a(f"- **Test(30%, OOS)**: {s['test']['n']}건, 승률 {s['test']['win_rate']}%, EV {s['test']['ev']}%")
            a(f"\n**연도별 성과:**")
            a("| 연도 | 거래 | 승률 | EV | 누적 |")
            a("|------|------|------|-----|------|")
            for y, d in s['yearly'].items():
                a(f"| {y} | {d['n']} | {d['win_rate']}% | {d['ev']}% | {d['total_pnl']}% |")
    else:
        a("\n**OOS(out-of-sample) EV > 0 이면서 전체 승률 40%+ 인 전략이 발견되지 않았습니다.**")
        a("\n→ 이는 단순 이벤트 기반 전략이 실제로 시장에서 통하지 않는다는 신호입니다. 추가 지표 조합이 필요합니다.")

    # 8. 한계점
    a("\n## 8. 한계점 및 주의사항")
    a(f"\n- 샘플 종목 {SAMPLE_SIZE}개로 제한 — 전체 시장 대비 편향 가능")
    a("- 생존자 편향: 상장폐지 종목 미포함")
    a(f"- 페니스톡 필터 적용: 전일 종가 < ${MIN_PRICE} OR 거래대금 < ${MIN_DOLLAR_VOL:,} 제외")
    a(f"- 수익률 클리핑: [{RET_CLIP_DOWN}%, +{RET_CLIP_UP}%] — 극단값 오염 방지")
    a("- D+1 시가 진입 + D+n 종가 청산 (실전 시뮬레이션)")
    a("- 실제 슬리피지/스프레드 미반영 (저가 종목일수록 실제 수익 낮아짐)")
    a("- TP/SL은 일봉 기준 — 장중 동시 도달 시 보수적(SL 우선) 처리")
    a("- Train(70%) / Test(30%) 시간순 분리로 look-ahead bias 방지")

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
