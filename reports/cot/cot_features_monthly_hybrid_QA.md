# COT Hybrid Monthly Features QA

- built_at_utc: `2025-12-12T21:25:28Z`
- schema_version: `cot_features_monthly_hybrid_v1`
- input: `data/cot/cot_store_weekly_unified.parquet`
- output: `data/cot/cot_features_monthly_hybrid.parquet`

## Coverage
- rows: **12,028**
- markets (market_key): **230**
- month min: **1986-01-31**
- month max: **2024-12-31**

### By regime
- dea_legacy: rows=2,586 | markets=132 | months=48
- tff: rows=9,442 | markets=98 | months=223

## Missingness (key outputs)
- cot_net_primary: nulls=0 (0.00%)
- cot_net_primary_pct_oi: nulls=9,442 (78.50%)
- cot_oi: nulls=0 (0.00%)
- cot_net_alt: nulls=2,586 (21.50%)
- cot_net_alt_pct_oi: nulls=12,028 (100.00%)
- cot_z_52w_primary: nulls=3,205 (26.65%)
- cot_z_52w_alt: nulls=3,205 (26.65%)

## Sanity checks
- PK candidate: `['month', 'market_key', 'cot_regime']` duplicates: **0**

## Samples
### Head
| month               |   market_key | cot_regime   |   cot_net_primary |   cot_net_primary_pct_oi |   cot_oi |   dea_traders_total |   dea_top4_long_pct |   cot_net_alt |   cot_net_alt_pct_oi |   cot_z_52w_primary |   cot_z_52w_alt | schema_version                 | built_at_utc         |
|:--------------------|-------------:|:-------------|------------------:|-------------------------:|---------:|--------------------:|--------------------:|--------------:|---------------------:|--------------------:|----------------:|:-------------------------------|:---------------------|
| 1986-01-31 00:00:00 |       105741 | dea_legacy   |              -252 |               -28.2511   |      892 |                   6 |                85.4 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       111651 | dea_legacy   |                97 |                 0.653815 |    14836 |                  66 |                31.2 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       111652 | dea_legacy   |              -130 |                -2.76419  |     4703 |                  39 |                40.8 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       120602 | dea_legacy   |               219 |                 5.03448  |     4350 |                  27 |                67.5 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       121601 | dea_legacy   |              2204 |                20.5866   |    10706 |                  61 |                33.7 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       132741 | dea_legacy   |              1240 |                 0.853536 |   145278 |                 203 |                22.6 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       136611 | dea_legacy   |              3367 |                20.3616   |    16536 |                  69 |                28.1 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 1986-01-31 00:00:00 |       138741 | dea_legacy   |             -3019 |                -4.74238  |    63660 |                  59 |                20.5 |           nan |                  nan |                 nan |             nan | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |

### Tail
| month               | market_key     | cot_regime   |   cot_net_primary |   cot_net_primary_pct_oi |           cot_oi |   dea_traders_total |   dea_top4_long_pct |       cot_net_alt |   cot_net_alt_pct_oi |   cot_z_52w_primary |   cot_z_52w_alt | schema_version                 | built_at_utc         |
|:--------------------|:---------------|:-------------|------------------:|-------------------------:|-----------------:|--------------------:|--------------------:|------------------:|---------------------:|--------------------:|----------------:|:-------------------------------|:---------------------|
| 2024-12-31 00:00:00 | ULTRA_UST_10Y  | tff          | -332613           |                      nan |      2.16134e+06 |                 nan |                 nan | -332613           |                  nan |          -2.24977   |      -2.24977   | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | ULTRA_UST_BOND | tff          | -529836           |                      nan |      1.76596e+06 |                 nan |                 nan | -529836           |                  nan |           1.81631   |       1.81631   | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | USD_INDEX      | tff          |   -9278           |                      nan |  42976           |                 nan |                 nan |   -9278           |                  nan |          -0.375603  |      -0.375603  | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | UST_10Y_NOTE   | tff          |      -1.5029e+06  |                      nan |      4.51368e+06 |                 nan |                 nan |      -1.5029e+06  |                  nan |           0.198169  |       0.198169  | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | UST_2Y_NOTE    | tff          |      -2.3081e+06  |                      nan |      4.28894e+06 |                 nan |                 nan |      -2.3081e+06  |                  nan |          -0.709919  |      -0.709919  | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | UST_5Y_NOTE    | tff          |      -2.84111e+06 |                      nan |      6.15822e+06 |                 nan |                 nan |      -2.84111e+06 |                  nan |          -0.499057  |      -0.499057  | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | UST_BOND       | tff          | -487025           |                      nan |      1.91482e+06 |                 nan |                 nan | -487025           |                  nan |          -0.558446  |      -0.558446  | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |
| 2024-12-31 00:00:00 | VIX_FUTURES    | tff          |  -15058           |                      nan | 275406           |                 nan |                 nan |  -15058           |                  nan |           0.0680114 |       0.0680114 | cot_features_monthly_hybrid_v1 | 2025-12-12T21:25:28Z |