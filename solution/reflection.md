# Reflection (≤1 page)

**Which fault types were hardest to catch, and why?**

1. **Subtle Distribution & Drift Faults (`distribution_shift`, `corpus_staleness`, `embedding_drift`, `null_spike`)**:
   - While catastrophic faults (e.g., schema breaks, missing upstream DAG dependencies) produce distinct, discrete violations, subtle faults manifest as minor statistical perturbations well within the $3\sigma$ bounds of normal operational variance.
   - For instance, in the private phase, subtle `corpus_staleness` instances exhibited average document ages around 35–37 days (against a baseline maximum of 49.8 days), and subtle `distribution_shift` showed mean amounts fluctuating within ordinary bounds.
   - Under an isolated single-event evaluation model, static scalar thresholds cannot distinguish these low-amplitude shifts from stochastic noise. Aggressively tightening thresholds risks triggering false alarms, which incur a heavy scoring penalty ($-0.3 \cdot \text{FPR}$) compared to the reward for catches ($+0.5 \cdot \text{TPR}$). Maintaining an empirical $\text{FPR} = 0.0$ on the private phase naturally required accepting lower sensitivity on these borderline perturbations.

2. **Lineage Runtime Jitter vs. Runtime Anomalies**:
   - Execution duration naturally fluctuates due to distributed compute noise, queue delays, and cluster load. Detecting subtle runtime regressions without per-job historical moving averages risks misidentifying transient worker latency as a genuine pipeline bottleneck.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

1. **Stateful Online Detection (EWMA via `ctx.state`)**:
   - In our Checkpoint 3 implementation, we introduced stateful tracking inside `ctx.state` (which incurs zero RPC credit cost) to record sliding-window metrics and compute Exponentially Weighted Moving Averages (EWMA with $\alpha = 0.3$) across incoming stream batches.
   - This architectural enhancement allows monitoring trend trajectories (e.g. cumulative staleness creep, drift velocity) and provides the foundation for sequential change-point detection (such as online CUSUM) across long-running production streams without degrading single-batch precision.

2. **Adaptive Two-Tier Screening & Budget Pacing**:
   - In our pipeline, every event dispatches a metered tool call (consuming 180 credits in practice, 240 in public, and 300 in private). In the public phase, this caused a minor budget overage ($240 / 220 = 9.09\%$). However, on the final private evaluation, the 300-credit spend was comfortably within the 320 budget (cost overage = $0.0$), yielding a final score of **33.33** with **TPR = 66.67%** and zero false alarms (**FPR = 0.0**).
   - In a production system with tighter budget constraints, we would implement dynamic budget throttling: monitor `ctx.tools.spend_so_far()` and `ctx.tools.budget_remaining()`, subsample high-cost checks (`feature_drift` and `embedding_drift` at 2.0 credits each) when burn rate exceeds stream pace, and prioritize checks on contracts and lineage where violation signals are deterministic and cost-effective.
