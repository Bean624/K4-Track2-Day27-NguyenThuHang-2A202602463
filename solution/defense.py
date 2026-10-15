"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.
"""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    batch_id = payload.get("batch_id")
    if not batch_id:
        return Verdict(alert=False, pillar="checks")

    profile = ctx.tools.batch_profile(batch_id)
    if "error" in profile:
        return Verdict(alert=False, pillar="checks", reason=profile["error"])

    b = ctx.baseline
    reasons = []

    row_count = profile.get("row_count", 0)
    if row_count < b.get("row_count_min", 0) or row_count > b.get("row_count_max", float("inf")):
        reasons.append(f"row_count {row_count} out of bounds")

    null_rate = profile.get("null_rate", {}).get("customer_id", 0)
    if null_rate > b.get("null_rate_max", float("inf")):
        reasons.append(f"null_rate {null_rate} exceeds max")

    mean_amount = profile.get("mean_amount", 0)
    if mean_amount < b.get("mean_amount_min", 0) or mean_amount > b.get("mean_amount_max", float("inf")):
        reasons.append(f"mean_amount {mean_amount} out of bounds")

    staleness_min = profile.get("staleness_min", 0)
    staleness_max = min(7.0, b.get("staleness_min_max", float("inf")))
    if staleness_min > staleness_max:
        reasons.append(f"staleness {staleness_min} exceeds max")

    if reasons:
        return Verdict(alert=True, pillar="checks", reason="; ".join(reasons))
    return Verdict(alert=False, pillar="checks")


def check_contract_checkpoint(payload, ctx):
    contract_id = payload.get("contract_id")
    checkpoint_batch_id = payload.get("checkpoint_batch_id")
    if not contract_id or not checkpoint_batch_id:
        return Verdict(alert=False, pillar="contracts")

    diff = ctx.tools.contract_diff(contract_id, checkpoint_batch_id)
    if "error" in diff:
        return Verdict(alert=False, pillar="contracts", reason=diff["error"])

    b = ctx.baseline
    violations = diff.get("violations", [])
    freshness_delay = diff.get("freshness_delay_min", 0)

    reasons = []
    if violations:
        reasons.append(f"contract violations: {violations}")
    if freshness_delay > b.get("freshness_delay_max_min", float("inf")):
        reasons.append(f"freshness_delay {freshness_delay} exceeds max")

    if reasons:
        return Verdict(alert=True, pillar="contracts", reason="; ".join(reasons))
    return Verdict(alert=False, pillar="contracts")


def check_lineage_run(payload, ctx):
    run_id = payload.get("run_id")
    if not run_id:
        return Verdict(alert=False, pillar="lineage")

    slice_res = ctx.tools.lineage_graph_slice(run_id)
    if "error" in slice_res:
        return Verdict(alert=False, pillar="lineage", reason=slice_res["error"])

    b = ctx.baseline
    reasons = []

    duration_ms = slice_res.get("duration_ms", 0)
    if duration_ms > b.get("lineage_duration_ms_max", float("inf")):
        reasons.append(f"duration {duration_ms}ms exceeds max")

    actual_upstream = slice_res.get("actual_upstream", [])
    if len(actual_upstream) < 2:
        reasons.append(f"missing upstream: got {actual_upstream}")

    actual_downstream = slice_res.get("actual_downstream_count", 0)
    if actual_downstream < 1:
        reasons.append("orphan output: downstream count < 1")

    if reasons:
        return Verdict(alert=True, pillar="lineage", reason="; ".join(reasons))
    return Verdict(alert=False, pillar="lineage")


def check_feature_materialization(payload, ctx):
    feature_view = payload.get("feature_view")
    batch_id = payload.get("batch_id")
    if not feature_view or not batch_id:
        return Verdict(alert=False, pillar="ai_infra")

    drift = ctx.tools.feature_drift(feature_view, batch_id)
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason=drift["error"])

    b = ctx.baseline
    mean_shift_sigma = drift.get("mean_shift_sigma", 0)
    sigma_max = max(0.60, b.get("feature_mean_shift_sigma_max", 0.4095))
    if mean_shift_sigma > sigma_max:
        return Verdict(alert=True, pillar="ai_infra", reason=f"mean_shift_sigma {mean_shift_sigma} exceeds max")
    return Verdict(alert=False, pillar="ai_infra")


def check_embedding_batch(payload, ctx):
    corpus = payload.get("corpus")
    chunk_batch_id = payload.get("chunk_batch_id")
    if not corpus or not chunk_batch_id:
        return Verdict(alert=False, pillar="ai_infra")

    drift = ctx.tools.embedding_drift(corpus, chunk_batch_id)
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason=drift["error"])

    b = ctx.baseline
    reasons = []

    centroid_shift = drift.get("centroid_shift", 0)
    shift_max = min(0.039, b.get("embedding_centroid_shift_max", float("inf")))
    if centroid_shift > shift_max:
        reasons.append(f"centroid_shift {centroid_shift} exceeds max")

    avg_doc_age_days = drift.get("avg_doc_age_days", 0)
    age_max = min(45.0, b.get("corpus_avg_doc_age_days_max", float("inf")))
    if avg_doc_age_days > age_max:
        reasons.append(f"avg_doc_age_days {avg_doc_age_days} exceeds max")

    if reasons:
        return Verdict(alert=True, pillar="ai_infra", reason="; ".join(reasons))
    return Verdict(alert=False, pillar="ai_infra")
