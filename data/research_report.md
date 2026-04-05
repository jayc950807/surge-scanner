# 자율 연구 에이전트 리포트 (LLM-Free v3)

생성일: 2026-04-05 09:56
분석 기간: 2020-04-01 ~ 2025-04-01
스캔 종목 수: 10000

## 1. 발견된 시장 이벤트 요약

| 이벤트 유형 | 설명 | 발견 건수 |
|------------|------|----------|
| week_surge_100 | 5일간 100%+ 상승 | 1483 |
| week_crash_50 | 5일간 50%+ 하락 | 1162 |
| gap_down_20 | 갭다운 20%+ | 911 |
| crash_30 | 하루 30%+ 급락 | 839 |
| gap_up_30 | 갭업 30%+ | 779 |
| surge_50 | 하루 50%+ 급등 | 535 |
| crash_50 | 하루 50%+ 급락 | 166 |
| rsi_extreme_low | RSI14 < 10 | 139 |
| bb_crash_below | 볼린저 하단 -50% 이탈 | 137 |
| surge_100 | 하루 100%+ 급등 | 123 |
| surge_200 | 하루 200%+ 급등 | 33 |

**전체 이벤트: 6307건**

## 2. 연도별 분포

### surge_100
| 연도 | 건수 |
|------|------|
| 2020 | 18 |
| 2021 | 30 |
| 2022 | 14 |
| 2023 | 9 |
| 2024 | 41 |
| 2025 | 11 |

### surge_50
| 연도 | 건수 |
|------|------|
| 2020 | 81 |
| 2021 | 120 |
| 2022 | 61 |
| 2023 | 70 |
| 2024 | 159 |
| 2025 | 44 |

### crash_30
| 연도 | 건수 |
|------|------|
| 2020 | 72 |
| 2021 | 103 |
| 2022 | 152 |
| 2023 | 162 |
| 2024 | 251 |
| 2025 | 99 |

### gap_up_30
| 연도 | 건수 |
|------|------|
| 2020 | 140 |
| 2021 | 161 |
| 2022 | 62 |
| 2023 | 106 |
| 2024 | 235 |
| 2025 | 75 |

### bb_crash_below
| 연도 | 건수 |
|------|------|
| 2020 | 5 |
| 2021 | 31 |
| 2022 | 30 |
| 2023 | 31 |
| 2024 | 31 |
| 2025 | 9 |

### week_crash_50
| 연도 | 건수 |
|------|------|
| 2020 | 75 |
| 2021 | 113 |
| 2022 | 234 |
| 2023 | 230 |
| 2024 | 376 |
| 2025 | 134 |

### week_surge_100
| 연도 | 건수 |
|------|------|
| 2020 | 259 |
| 2021 | 225 |
| 2022 | 126 |
| 2023 | 234 |
| 2024 | 507 |
| 2025 | 132 |

### gap_down_20
| 연도 | 건수 |
|------|------|
| 2020 | 98 |
| 2021 | 140 |
| 2022 | 147 |
| 2023 | 191 |
| 2024 | 242 |
| 2025 | 93 |

### crash_50
| 연도 | 건수 |
|------|------|
| 2020 | 13 |
| 2021 | 18 |
| 2022 | 33 |
| 2023 | 32 |
| 2024 | 54 |
| 2025 | 16 |

### surge_200
| 연도 | 건수 |
|------|------|
| 2020 | 7 |
| 2021 | 7 |
| 2022 | 2 |
| 2023 | 3 |
| 2024 | 10 |
| 2025 | 4 |

### rsi_extreme_low
| 연도 | 건수 |
|------|------|
| 2020 | 1 |
| 2021 | 55 |
| 2022 | 29 |
| 2023 | 35 |
| 2024 | 19 |

## 3. 이벤트 유형별 수익성 (트림평균 기준, 극단값 제거)

| 이벤트 | 기간 | 건수 | 승률 | 중앙값 | 트림평균(EV) |
|--------|------|------|------|--------|------|
| rsi_extreme_low | post_3d_ret | 139 | 46.0% | -0.6% | 0.97% |
| rsi_extreme_low | post_5d_ret | 139 | 51.1% | 0.16% | 0.22% |
| rsi_extreme_low | post_1d_ret | 139 | 46.8% | -0.47% | -0.19% |
| bb_crash_below | post_1d_ret | 137 | 43.1% | -0.83% | -1.18% |
| gap_down_20 | post_1d_ret | 911 | 41.4% | -1.39% | -1.41% |
| bb_crash_below | post_5d_ret | 137 | 51.8% | 0.39% | -1.53% |
| bb_crash_below | post_3d_ret | 137 | 46.0% | -0.6% | -1.8% |
| week_crash_50 | post_1d_ret | 1162 | 35.4% | -2.24% | -2.18% |
| gap_up_30 | post_1d_ret | 779 | 38.4% | -2.69% | -2.4% |
| crash_30 | post_1d_ret | 839 | 37.3% | -2.81% | -2.51% |
| week_surge_100 | post_1d_ret | 1483 | 37.6% | -3.59% | -2.75% |
| gap_down_20 | post_3d_ret | 907 | 39.3% | -3.16% | -3.16% |
| week_crash_50 | post_3d_ret | 1160 | 36.6% | -4.35% | -4.34% |
| gap_down_20 | post_5d_ret | 905 | 38.2% | -4.0% | -4.47% |
| crash_30 | post_3d_ret | 835 | 36.2% | -5.51% | -4.78% |
| surge_50 | post_1d_ret | 535 | 33.1% | -6.56% | -5.76% |
| week_crash_50 | post_5d_ret | 1154 | 34.5% | -6.21% | -5.88% |
| crash_50 | post_1d_ret | 166 | 25.9% | -6.29% | -6.69% |
| gap_up_30 | post_3d_ret | 778 | 33.2% | -6.64% | -6.71% |
| crash_30 | post_5d_ret | 833 | 35.2% | -6.84% | -6.87% |
| week_surge_100 | post_3d_ret | 1481 | 32.5% | -8.78% | -6.93% |
| crash_50 | post_3d_ret | 165 | 31.5% | -9.4% | -8.19% |
| gap_up_30 | post_5d_ret | 777 | 31.7% | -9.34% | -9.29% |
| week_surge_100 | post_5d_ret | 1477 | 31.8% | -11.36% | -9.53% |
| surge_50 | post_3d_ret | 535 | 30.1% | -11.62% | -9.69% |
| surge_100 | post_1d_ret | 123 | 22.8% | -10.57% | -10.68% |
| crash_50 | post_5d_ret | 165 | 32.1% | -12.5% | -11.37% |
| surge_50 | post_5d_ret | 534 | 29.4% | -16.66% | -12.94% |
| surge_200 | post_1d_ret | 33 | 18.2% | -16.88% | -13.12% |
| surge_100 | post_3d_ret | 123 | 23.6% | -17.92% | -15.61% |

## 4. 핵심 공통점 (지표 집중도)

| 이벤트 | 지표 | 유형 | 상세 |
|--------|------|------|------|
| surge_100 | pre_macd | concentrated | 중앙값: 0.055, IQR비율: 0.1% |
| surge_100 | pre_high_60d | concentrated | 중앙값: 53.41, IQR비율: 0.1% |
| gap_up_30 | pre_macd | concentrated | 중앙값: -0.079, IQR비율: 0.1% |
| surge_100 | pre_bb_upper | concentrated | 중앙값: 30.963, IQR비율: 0.2% |
| surge_100 | pre_macd_hist | concentrated | 중앙값: 0.25, IQR비율: 0.2% |
| surge_100 | pre_sma_5 | concentrated | 중앙값: 21.12, IQR비율: 0.2% |
| surge_100 | pre_sma_20 | concentrated | 중앙값: 20.497, IQR비율: 0.2% |
| surge_100 | pre_sma_50 | concentrated | 중앙값: 25.926, IQR비율: 0.2% |
| surge_100 | pre_high_20d | concentrated | 중앙값: 45.15, IQR비율: 0.2% |
| surge_100 | pre_low_20d | concentrated | 중앙값: 12.8, IQR비율: 0.2% |
| surge_50 | pre_sma_50 | concentrated | 중앙값: 18.6, IQR비율: 0.2% |
| crash_30 | pre_atr_14 | concentrated | 중앙값: 3.241, IQR비율: 0.2% |
| gap_up_30 | pre_bb_lower | concentrated | 중앙값: 11.795, IQR비율: 0.2% |
| gap_up_30 | pre_macd_hist | concentrated | 중앙값: 0.18, IQR비율: 0.2% |
| gap_up_30 | pre_sma_50 | concentrated | 중앙값: 25.508, IQR비율: 0.2% |
| gap_up_30 | pre_high_60d | concentrated | 중앙값: 55.5, IQR비율: 0.2% |
| week_surge_100 | pre_macd_hist | concentrated | 중앙값: 1.108, IQR비율: 0.2% |
| week_surge_100 | pre_sma_5 | concentrated | 중앙값: 14.188, IQR비율: 0.2% |
| week_surge_100 | pre_sma_20 | concentrated | 중앙값: 10.095, IQR비율: 0.2% |
| week_surge_100 | pre_high_20d | concentrated | 중앙값: 28.37, IQR비율: 0.2% |
| week_surge_100 | pre_low_20d | concentrated | 중앙값: 6.2, IQR비율: 0.2% |
| week_surge_100 | pre_high_60d | concentrated | 중앙값: 30.64, IQR비율: 0.2% |
| crash_50 | pre_macd | concentrated | 중앙값: 0.268, IQR비율: 0.2% |
| surge_200 | pre_bb_upper | concentrated | 중앙값: 20.72, IQR비율: 0.2% |
| surge_200 | pre_atr_14 | concentrated | 중앙값: 2.514, IQR비율: 0.2% |
| surge_200 | pre_sma_20 | concentrated | 중앙값: 14.477, IQR비율: 0.2% |
| surge_200 | pre_sma_50 | concentrated | 중앙값: 14.254, IQR비율: 0.2% |
| surge_200 | pre_high_20d | concentrated | 중앙값: 29.2, IQR비율: 0.2% |
| surge_200 | pre_low_20d | concentrated | 중앙값: 8.55, IQR비율: 0.2% |
| surge_100 | pre_bb_lower | concentrated | 중앙값: 9.404, IQR비율: 0.3% |

## 5. ML 자동 패턴 발견 (class_weight='balanced' 적용)

### post_1d_ret 수익 예측
- 샘플: 6307건 | 기본 승률: 37.0%
- Random Forest F1(macro): 0.569 | 정확도: 58.7%
- Decision Tree F1(macro): 0.514
- 수익 시 중앙값: +6.73% | 손실 시 중앙값: -7.76%

**중요 지표 Top 10:**

| 지표 | 중요도 |
|------|--------|
| pre_atr_14 | 0.0488 |
| pre_volatility_20d | 0.047 |
| pre_high_20d | 0.0444 |
| pre_high_60d | 0.0427 |
| pre_avg_vol_20d | 0.0347 |
| pre_dist_60d_high | 0.0346 |
| pre_obv | 0.0331 |
| pre_volatility_5d | 0.0306 |
| pre_atr_pct | 0.0297 |
| pre_avg_vol_10d | 0.029 |

**Decision Tree 규칙:**
```
|--- pre_high_20d <= 47.07
|   |--- pre_obv_slope_5d <= -2.55
|   |   |--- pre_atr_pct <= 8.92
|   |   |   |--- class: 1
|   |   |--- pre_atr_pct >  8.92
|   |   |   |--- pre_ret_40d <= -44.60
|   |   |   |   |--- class: 1
|   |   |   |--- pre_ret_40d >  -44.60
|   |   |   |   |--- class: 0
|   |--- pre_obv_slope_5d >  -2.55
|   |   |--- pre_ret_40d <= -31.16
|   |   |   |--- class: 0
|   |   |--- pre_ret_40d >  -31.16
|   |   |   |--- pre_ret_40d <= 7.99
|   |   |   |   |--- class: 1
|   |   |   |--- pre_ret_40d >  7.99
|   |   |   |   |--- class: 1
|--- pre_high_20d >  47.07
|   |--- pre_atr_pct <= 6.56
|   |   |--- class: 1
|   |--- pre_atr_pct >  6.56
|   |   |--- pre_rsi_7 <= 81.65
|   |   |   |--- pre_bb_position <= 0.65
|   |   |   |   |--- class: 0
|   |   |   |--- pre_bb_position >  0.65
|   |   |   |   |--- class: 0
|   |   |--- pre_rsi_7 >  81.65
|   |   |   |--- pre_obv <= 2220707.50
|   |   |   |   |--- class: 1
|   |   |   |--- pre_obv >  2220707.50
|   |   |   |   |--- class: 0

```

### post_3d_ret 수익 예측
- 샘플: 6293건 | 기본 승률: 34.9%
- Random Forest F1(macro): 0.592 | 정확도: 62.9%
- Decision Tree F1(macro): 0.532
- 수익 시 중앙값: +11.02% | 손실 시 중앙값: -13.64%

**중요 지표 Top 10:**

| 지표 | 중요도 |
|------|--------|
| pre_atr_pct | 0.0604 |
| pre_intraday_range | 0.0515 |
| pre_volatility_20d | 0.0485 |
| pre_obv | 0.0419 |
| pre_high_20d | 0.0406 |
| pre_atr_14 | 0.0397 |
| pre_avg_vol_10d | 0.0317 |
| pre_avg_vol_20d | 0.031 |
| pre_dist_60d_high | 0.0306 |
| pre_avg_vol_5d | 0.0294 |

**Decision Tree 규칙:**
```
|--- pre_volatility_20d <= 5.75
|   |--- pre_avg_vol_5d <= 100782.90
|   |   |--- class: 0
|   |--- pre_avg_vol_5d >  100782.90
|   |   |--- pre_ret_20d <= -3.80
|   |   |   |--- pre_ret_60d <= -21.68
|   |   |   |   |--- class: 1
|   |   |   |--- pre_ret_60d >  -21.68
|   |   |   |   |--- class: 1
|   |   |--- pre_ret_20d >  -3.80
|   |   |   |--- pre_ret_5d <= 3.52
|   |   |   |   |--- class: 1
|   |   |   |--- pre_ret_5d >  3.52
|   |   |   |   |--- class: 1
|--- pre_volatility_20d >  5.75
|   |--- pre_high_20d <= 56.42
|   |   |--- pre_ret_5d <= -38.91
|   |   |   |--- class: 1
|   |   |--- pre_ret_5d >  -38.91
|   |   |   |--- pre_dist_60d_high <= -26.47
|   |   |   |   |--- class: 0
|   |   |   |--- pre_dist_60d_high >  -26.47
|   |   |   |   |--- class: 1
|   |--- pre_high_20d >  56.42
|   |   |--- pre_bb_upper <= 49.80
|   |   |   |--- class: 0
|   |   |--- pre_bb_upper >  49.80
|   |   |   |--- pre_volatility_5d <= 6.81
|   |   |   |   |--- class: 0
|   |   |   |--- pre_volatility_5d >  6.81
|   |   |   |   |--- class: 0

```

### post_5d_ret 수익 예측
- 샘플: 6277건 | 기본 승률: 34.0%
- Random Forest F1(macro): 0.59 | 정확도: 63.3%
- Decision Tree F1(macro): 0.529
- 수익 시 중앙값: +12.22% | 손실 시 중앙값: -17.18%

**중요 지표 Top 10:**

| 지표 | 중요도 |
|------|--------|
| pre_atr_pct | 0.0723 |
| pre_volatility_20d | 0.0693 |
| pre_atr_14 | 0.062 |
| pre_intraday_range | 0.0576 |
| pre_rsi_14 | 0.0419 |
| pre_bb_width | 0.0342 |
| pre_avg_vol_20d | 0.0323 |
| pre_high_60d | 0.031 |
| pre_dist_20d_low | 0.0306 |
| pre_high_20d | 0.0285 |

**Decision Tree 규칙:**
```
|--- pre_volatility_20d <= 7.97
|   |--- pre_avg_vol_20d <= 45080.18
|   |   |--- pre_dist_20d_low <= 11.29
|   |   |   |--- class: 1
|   |   |--- pre_dist_20d_low >  11.29
|   |   |   |--- class: 0
|   |--- pre_avg_vol_20d >  45080.18
|   |   |--- pre_high_60d <= 8.32
|   |   |   |--- class: 1
|   |   |--- pre_high_60d >  8.32
|   |   |   |--- pre_sma_5 <= 9.32
|   |   |   |   |--- class: 1
|   |   |   |--- pre_sma_5 >  9.32
|   |   |   |   |--- class: 1
|--- pre_volatility_20d >  7.97
|   |--- pre_rsi_14 <= 25.23
|   |   |--- pre_high_20d <= 62.08
|   |   |   |--- class: 1
|   |   |--- pre_high_20d >  62.08
|   |   |   |--- class: 0
|   |--- pre_rsi_14 >  25.23
|   |   |--- pre_ret_60d <= 34.19
|   |   |   |--- pre_avg_vol_10d <= 2007242.81
|   |   |   |   |--- class: 0
|   |   |   |--- pre_avg_vol_10d >  2007242.81
|   |   |   |   |--- class: 0
|   |   |--- pre_ret_60d >  34.19
|   |   |   |--- pre_volatility_20d <= 15.76
|   |   |   |   |--- class: 1
|   |   |   |--- pre_volatility_20d >  15.76
|   |   |   |   |--- class: 0

```

## 6. 연도별 안정성 (일관된 패턴)

모든 연도에서 일관된 패턴은 발견되지 않았습니다.

## 7. 발견된 전략 후보 (Train/Test 분리 + 연도별 분해)

**OOS(out-of-sample) EV > 0 이면서 전체 승률 40%+ 인 전략이 발견되지 않았습니다.**

→ 이는 단순 이벤트 기반 전략이 실제로 시장에서 통하지 않는다는 신호입니다. 추가 지표 조합이 필요합니다.

## 8. 한계점 및 주의사항

- 샘플 종목 10000개로 제한 — 전체 시장 대비 편향 가능
- 생존자 편향: 상장폐지 종목 미포함
- 페니스톡 필터 적용: 전일 종가 < $2.0 OR 거래대금 < $500,000 제외
- 수익률 클리핑: [-95.0%, +300.0%] — 극단값 오염 방지
- D+1 시가 진입 + D+n 종가 청산 (실전 시뮬레이션)
- 실제 슬리피지/스프레드 미반영 (저가 종목일수록 실제 수익 낮아짐)
- TP/SL은 일봉 기준 — 장중 동시 도달 시 보수적(SL 우선) 처리
- Train(70%) / Test(30%) 시간순 분리로 look-ahead bias 방지