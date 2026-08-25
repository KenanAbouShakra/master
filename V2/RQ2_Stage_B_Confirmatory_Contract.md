RQ2 Stage-B Confirmatory Evaluation Contract

Protocol status: Draft for review; not authorised for execution
Protocol date: 25 August 2026
Protected evidence: Measurement, Stage A, and RQ1 remain frozen.

1. Final RQ2

RQ2: How do deviation magnitude, duration, dimensionality, data volume, and missingness affect the sensitivity, timeliness, and alarm burden of repository-relative CI monitoring, and how stable are these operating characteristics across repositories?

This formulation does not assume that monitoring is reliable, does not select a winning detector, and does not require an arbitrary universal threshold for the word reliable. It defines observable operating characteristics that can be estimated with uncertainty.

2. Rationale for Stage B

Stage A used 42,300 evaluable synthetic scenarios from the three development repositories and produced 5,118,300 raw scenario-detector rows. It was frozen before the revised RQ2 was adopted. Stage A therefore remains valid development and feasibility evidence, but it is not relabelled as the sole confirmatory answer to the amended RQ2.

Stage B is a prospective external-repository evaluation. It uses the three external repositories and new locked random seeds. All detector parameters remain exactly as frozen.

3. Analysis unit and evidence domain

series: repository-week CI evidence;

repositories: containerd/containerd, helm/helm, and pytest-dev/pytest;

calibration: the first 13 observed weeks already frozen for each external repository;

injection and scoring: only weeks marked external_evaluation_eligible;

scenario unit: one external repository, condition cell, and repetition;

detector unit: one frozen primary detector variant applied to one scenario.

External repositories are not used to choose detector parameters, scenario magnitudes, durations, outcome definitions, or reporting rules.

4. Frozen monitoring procedures

Primary causal rolling MAD variants

history window: 13 weeks;

scaled-MAD threshold: 3.0;

persistence: 2 weeks;

variants: latency_log, failure_rate, rerun_rate, union, and two_of_three.

Primary MEWMA

lambda: 0.20;

empirical control limit: 7.150034791553729;

standardisation: frozen repository-specific parameters;

refitting during Stage B: prohibited.

Sensitivity MAD specifications are not part of the primary confirmatory comparison. They may be evaluated later only under a separately labelled sensitivity output.

5. Scenario factors

Stage B retains the magnitude semantics used in the frozen design:

latency relative shifts: 10%, 25%, and 50%, implemented as latency_log + log1p(shift);

failure absolute probability shifts: 0.05, 0.10, and 0.20;

rerun absolute probability shifts: 0.05, 0.10, and 0.20;

durations: 1, 2, 4, and 8 weeks;

affected dimensions: every non-empty combination of latency, failure, and rerun;

observed-volume profiles: low, medium, and high magnitude;

low-volume profile: medium magnitude, 25% seeded denominator subsampling with minimum denominator 5;

controlled-missingness profile: medium magnitude, 25% of affected cells with a minimum of one cell.

One-week injections are retained as a transient comparator. They are not described as sustained deviations. Workflow-composition injection remains unsupported and excluded.

Each ordinary design cell receives 100 repetitions. Injection starts and stochastic count realisations use a new Stage-B base seed and stable hashed sub-seeds. Stage-A seeds must not be reused.

6. Paired counterfactual evaluation

Each injected scenario is evaluated against the same unaltered external series.

For detector (d), repository (r), scenario (s), and week (t), let

[
A^{(0)}_{drt}
]

be the alarm on the unaltered reference series and

[
A^{(1)}_{dsrt}
]

the alarm after injection. The injection-attributable binary alarm response is

[
I_{dsrt}=A^{(1)}{dsrt}\land \neg A^{(0)}{drt}.
]

This paired definition prevents pre-existing alarms in real repository data from automatically being labelled false positives. The original Stage-A measures remain frozen and unchanged; Stage B introduces new names rather than rewriting Stage A.

7. Primary estimands

7.1 Strict incremental episode sensitivity

[
P{\exists t\in E_s:I_{dsrt}=1},
]

where (E_s) is the injected interval. This estimates whether the injection creates at least one new alarm during the affected interval.

7.2 Strict incremental detection delay

For scenarios with an incremental alarm inside the injected interval:

[
D_{dsr}=\min{t\in E_s:I_{dsrt}=1}-\min(E_s).
]

Undetected scenarios are not assigned a numerical delay. Detection probability and delay are always reported together to avoid survivor-only interpretation.

7.3 Reference alarm burden

The fraction of externally eligible weeks alarmed on the unaltered reference series. This is descriptive alarm burden, not a false-alarm rate because real-world ground truth is unavailable.

7.4 Injection-attributable spillover burden

The number or fraction of new alarms after the injected interval that were absent from the paired reference alarm sequence. This quantifies persistence or memory beyond the controlled deviation interval without calling every off-interval alarm false.

8. Secondary estimands

total operational episode detection using (A^{(1)}), regardless of whether the corresponding week was already alarmed in the reference;

reference-alarm overlap within the chosen injection interval;

incremental alarm duration;

strict boundary overlap for incremental alarms;

evaluable and unevaluable fractions;

probability clipping frequency;

metric-specific denominator support;

an extended response-window analysis, separately labelled, for alarms appearing immediately after a short injection because of persistence or MEWMA memory.

Classical injection-relative precision and false-alarm rate may be reproduced for comparability with Stage A, but they are secondary and explicitly labelled as injection-relative rather than real-world truth.

9. Cross-repository stability

Every primary estimand is reported:

separately by repository;

separately by detector variant;

across magnitude, duration, dimensionality, volume, and missingness conditions;

with uncertainty based on scenario repetitions.

Pooled estimates may be reported only alongside repository-specific estimates. Three repositories are insufficient for ordinary cluster-robust asymptotics. No claim of universal transportability is made from a pooled average alone.

10. Statistical analysis

The raw Stage-B table is the source of truth. Aggregates must be exactly reconstructable from it.

binary incremental detection: binomial proportions with interval estimates and a pre-declared model that includes detector, condition factors, and repository;

delay: distributional summaries plus a time-to-detection analysis that retains undetected episodes as censored, if implementation diagnostics support it;

alarm burden and spillover: repository- and detector-specific rates with scenario-level uncertainty;

interactions: limited to scientifically defined detector-by-condition terms; no unrestricted search for significant subgroups;

multiplicity: effect sizes and uncertainty are primary; any family of hypothesis tests requires a frozen correction rule.

No single weighted score will be used to declare a winner. Sensitivity, timeliness, alarm burden, and evaluability form an operating profile.

11. Required raw outputs

Stage B must write to a new directory and must refuse overwrite. Required artifacts are:

scenario registry;

paired reference/injected week-level alarms;

one raw result row per scenario and primary detector;

pre-declared aggregate tables;

metadata with input hashes, code hashes, seed contract, detector contract, and diagnostic counts;

validation status;

a final freeze manifest created only after review.

12. Gates before execution

Stage B may run only after:

upstream manifests and all protected files pass hash verification;

compressed Stage-A raw results are audited without modification;

the Stage-B scenario grid is generated in planning mode and its size and strata are reviewed;

synthetic unit tests pass;

reference and injected alarm pairing is tested;

calibration weeks are excluded from injection and scoring;

detector parameters are proven unchanged;

output paths cannot collide with frozen files;

the configuration, code, expected schema, and seed namespace are frozen.

13. Interpretation boundary

Stage B estimates responsiveness to controlled perturbations of measured CI evidence. It does not establish real-world incident ground truth, identify root causes, demonstrate operational degradation, or prove that an alarm requires intervention.

