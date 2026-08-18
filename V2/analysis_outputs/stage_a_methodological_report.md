# Stage-A Synthetic Baseline Evaluation

## Denominator Audit

All 5,118,300 detector rows were reconstructed. Saved metric-specific counts matched the raw results, and stratum means matched within 1.89e-15. Every higher-level mean now uses its own valid-observation count.

## Frozen Primary Comparison

| Repository | Detector | Precision | Recall | False-alarm rate | Episode detection | Delay (weeks) | Boundary overlap | Unevaluable fraction |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| docker/cli | MAD union | 0.229 | 0.567 | 0.310 | 0.677 | 0.493 | 0.195 | 0.423 |
| docker/cli | MAD two-of-three | 1.000 | 0.144 | 0.000 | 0.276 | 1.188 | 0.144 | 0.423 |
| docker/cli | MEWMA | 0.494 | 0.700 | 0.062 | 0.816 | 0.413 | 0.424 | 0.000 |
| prometheus/prometheus | MAD union | 0.640 | 0.472 | 0.021 | 0.634 | 0.943 | 0.409 | 0.423 |
| prometheus/prometheus | MAD two-of-three | 1.000 | 0.106 | 0.000 | 0.210 | 1.094 | 0.106 | 0.423 |
| prometheus/prometheus | MEWMA | 0.738 | 0.333 | 0.021 | 0.507 | 1.596 | 0.267 | 0.000 |
| tektoncd/pipeline | MAD union | 0.517 | 0.354 | 0.031 | 0.553 | 0.734 | 0.281 | 0.423 |
| tektoncd/pipeline | MAD two-of-three | 1.000 | 0.022 | 0.000 | 0.061 | 1.228 | 0.022 | 0.423 |
| tektoncd/pipeline | MEWMA | 0.649 | 0.180 | 0.013 | 0.321 | 2.399 | 0.153 | 0.000 |

Primary MAD remains window 13, threshold 3.0, persistence 2. MEWMA remains frozen. Sensitivity results remain separate.

## Interpretation

- MEWMA remains strongest for sustained multiweek shifts and has near-complete evaluability.
- MAD union remains more sensitive than two-of-three but incurs more false alarms, especially for Docker.
- MAD two-of-three remains highly specific but misses many single-metric and short episodes.
- Larger and longer perturbations improve recall and episode detection.
- Controlled missingness reduces MAD evaluability; missing observations are not negatives.
- Correct metric-specific weighting changes some reported values but not these qualitative conclusions.

## Limitations

- Synthetic perturbations are not real-world ground truth.
- Workflow-composition injection remains unsupported.
- Probability shifts use seeded binomial realization.
- Detection delay is conditional on detection and uses a smaller denominator.
- Full-stratum tables govern interpretation; macro values alone are insufficient.

## Freeze Recommendation

Stage A remains ready to freeze as an evidence package. This does not select a detector or authorize tuning.
