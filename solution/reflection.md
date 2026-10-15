# Reflection (≤1 page)

**Which fault types were hardest to catch, and why?**

1. **Subtle Distribution & Drift Faults (`distribution_shift`, `corpus_staleness`, `embedding_drift`, `null_spike`)**:
   - While catastrophic faults (e.g., schema breaks, missing upstream DAG dependencies) produce distinct, discrete violations, subtle faults manifest as minor statistical perturbations well within the $3\sigma$ bounds of normal operational variance.
   - For instance, in the private phase, subtle `corpus_staleness` instances exhibited average document ages around 35–37 days (against a baseline maximum of 49.8 days), and subtle `distribution_shift` showed mean amounts fluctuating within ordinary bounds.
   - Under an isolated single-event evaluation model, static scalar thresholds cannot distinguish these low-amplitude shifts from stochastic noise. Aggressively tightening thresholds risks triggering false alarms, which incur a heavy scoring penalty ($-0.3 \cdot \text{FPR}$) compared to the reward for catches ($+0.5 \cdot \text{TPR}$). Maintaining an empirical $\text{FPR} = 0.0$ on the private phase naturally required accepting lower sensitivity on these borderline perturbations.

2. **Lineage Runtime Jitter vs. Runtime Anomalies**:
   - Execution duration naturally fluctuates due to distributed compute noise, queue delays, and cluster load. Detecting subtle runtime regressions without per-job historical moving averages risks misidentifying transient worker latency as a genuine pipeline bottleneck.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

1. **Stateful Online Detection (EWMA / CUSUM via `ctx.state`)**:
   - Rather than treating each event in isolation, I would leverage `ctx.state` (which incurs zero RPC cost) to maintain sliding-window Exponentially Weighted Moving Averages (EWMA) and Cumulative Sum (CUSUM) control charts across sequential batches.
   - This would enable detecting subtle cumulative drifts (gradual corpus aging or steady distribution drift) across consecutive runs without lowering single-batch thresholds or increasing false positives.

2. **Adaptive Two-Tier Screening & Budget Pacing**:
   - In our current implementation, every event dispatches a metered tool call (consuming 180 credits in practice, 240 in public, and 300 in private). In the public phase, this caused a minor budget overage ($240 / 220 = 9.09\%$), incurring a cost penalty.
   - With another pass, I would implement dynamic budget throttling: monitor `ctx.tools.spend_so_far()` and `ctx.tools.budget_remaining()`, subsample high-cost checks (`feature_drift` and `embedding_drift` at 2.0 credits each) when the burn rate exceeds the stream pace, and prioritize checks on contracts and lineage where violation signals are deterministic and cost-effective.
