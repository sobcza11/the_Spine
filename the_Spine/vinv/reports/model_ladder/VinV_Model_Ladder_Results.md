# VinV — Model Ladder Results (Walk-Forward)

Run UTC: 2025-12-15T21:22:07.653985

| model | fold | auc | logloss | brier | lift_top_decile | long_only_mean_m | long_short_mean_m | long_only_sharpe_ann | long_short_sharpe_ann |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tier0_dummy | 3.5000 | 0.5000 | 0.6434 | 0.2253 | 1.4667 |  |  |  |  |
| tier1_elastic_net | 3.5000 | 0.5000 | 5.3727 | 0.3889 | 1.4667 |  |  |  |  |
| tier1_logistic_l2 | 3.5000 | 1.0000 | 0.3955 | 0.1381 | 1.4667 |  |  |  |  |
| tier2_gbdt | 3.5000 | 1.0000 | 0.0000 | 0.0000 | 1.4667 |  |  |  |  |
| tier2_random_forest | 3.5000 | 1.0000 | 0.4832 | 0.1522 | 1.4667 |  |  |  |  |