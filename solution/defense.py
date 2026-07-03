"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.

Strategy: one metered tool call per event. A verdict combines two general,
seed-independent signals — neither hardcodes a magnitude or an event id:

1. Baseline breach (ctx.baseline) — mean +/- 3 sigma from real clean-stream
   measurements. Reliable for "obvious" tier faults.
2. Self-calibration — a running mean/std of this run's own *presumed-clean*
   readings per signal (Welford, O(1) per point), z-scored at a conservative
   threshold. This adapts to whatever population a given run actually has,
   which is what catches "subtle" faults sitting close to (or undercut by)
   the published baseline, without needing to know exact fault magnitudes
   ahead of time.

Only "presumed clean" points (ones that didn't already trip a flag) are
folded into a signal's running stats, so a run of faults can't drag the
"normal" model toward itself and mask further faults of the same kind. The
z-score threshold is deliberately conservative (well above the "obvious"
tier's baseline overshoot) since a small early sample tends to underestimate
true variance — a loose threshold here trades FPR for TPR faster than it's
worth against the scoring weights (0.5*TPR vs 0.3*FPR).
"""
import math
from api import Verdict

MIN_N = 10
Z_THRESH = 3.0
COMBO_PROXIMITY = 0.75   # each contributing metric must be within 25% of its own boundary
COMBO_MIN_SIGNALS = 2    # this many simultaneously-elevated metrics count as one alert
SOLO_PROXIMITY = 0.90    # a single metric this close to its own boundary is evidence on its own


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


# ---------- self-calibration helpers (kept in ctx.state, free to use) ----------

def _stats(ctx, key):
    return ctx.state.setdefault("_stats", {}).setdefault(key, {"n": 0, "mean": 0.0, "m2": 0.0})


def _fold_in(s, value):
    """Welford online update — mutates s in place."""
    n1 = s["n"] + 1
    delta = value - s["mean"]
    new_mean = s["mean"] + delta / n1
    s["m2"] += delta * (value - new_mean)
    s["n"], s["mean"] = n1, new_mean


def evaluate(ctx, key, value, hard_max=None, hard_min=None, zscore=True):
    """Returns (flagged, reason_suffix). Checks the hard baseline first, then
    (unless `zscore=False`) a z-score against this run's own running
    mean/std for the same signal. The running stats are only updated with
    `value` when it wasn't flagged, so faults never contaminate the "normal"
    model. `zscore=False` is for signals that are already a normalized
    sigma/ratio (e.g. feature_mean_shift_sigma) where the published baseline
    IS the statistical threshold — re-z-scoring an already-normalized ratio
    against this run's own noise just adds false positives, not signal.
    """
    s = _stats(ctx, key)
    n, mean = s["n"], s["mean"]
    std = math.sqrt(s["m2"] / (n - 1)) if n > 1 else None

    hard_flag = (hard_max is not None and value > hard_max) or \
                (hard_min is not None and value < hard_min)
    z_flag = zscore and n >= MIN_N and std and std > 1e-9 and abs(value - mean) / std > Z_THRESH

    flagged = hard_flag or z_flag
    if not flagged and zscore:
        _fold_in(s, value)

    if hard_flag:
        return True, "hard"
    if z_flag:
        return True, "zscore"
    return False, ""


def _proximity(value, hard_max=None, hard_min=None):
    """How close `value` sits to its baseline boundary, normalized so 1.0 ==
    exactly at the boundary (already caught by the hard check) and 0 == dead
    center. Used only to combine several simultaneously-elevated-but-not-yet-
    crossed metrics into one alert — a single metric at 0.9 proximity proves
    nothing on its own, but several at once on the same event is the
    "combining signals" approach FAULT_PILLARS.md/TOOLKIT_API.md point at."""
    if hard_max is not None and hard_min is not None:
        mid, half = (hard_max + hard_min) / 2, (hard_max - hard_min) / 2
        return abs(value - mid) / half if half > 0 else 0.0
    if hard_max is not None:
        return value / hard_max if hard_max > 0 else 0.0
    if hard_min is not None and value > 0:
        return hard_min / value
    return 0.0


def _mode_track(ctx, key, value):
    """Track the most-common observed value for a discrete signal (e.g.
    upstream-edge count per job) and flag drops below it once established.
    General across any job/table naming — no fixed count is hardcoded."""
    counts = ctx.state.setdefault("_modes", {}).setdefault(key, {})
    counts[value] = counts.get(value, 0) + 1
    return max(counts, key=counts.get)


# ---------------------------- handlers --------------------------------

def check_data_batch(payload, ctx):
    r = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in r:
        return Verdict(alert=False, pillar="checks", reason=r["error"])

    b = ctx.baseline
    row_count = r["row_count"]
    null_rate = r["null_rate"]["customer_id"]
    mean_amount = r["mean_amount"]
    staleness = r["staleness_min"]

    reasons = []
    f, _ = evaluate(ctx, "row_count", row_count, hard_max=b["row_count_max"], hard_min=b["row_count_min"])
    if f:
        reasons.append("volume")
    if null_rate > b["null_rate_max"]:
        reasons.append("null_spike")
    f, _ = evaluate(ctx, "mean_amount", mean_amount, hard_max=b["mean_amount_max"], hard_min=b["mean_amount_min"])
    if f:
        reasons.append("distribution_shift")
    f, _ = evaluate(ctx, "staleness", staleness, hard_max=b["staleness_min_max"])
    if f:
        reasons.append("freshness_lag")

    if not reasons:
        proximities = [
            _proximity(row_count, b["row_count_max"], b["row_count_min"]),
            _proximity(null_rate, b["null_rate_max"]),
            _proximity(mean_amount, b["mean_amount_max"], b["mean_amount_min"]),
            _proximity(staleness, b["staleness_min_max"]),
        ]
        if sum(1 for p in proximities if p >= COMBO_PROXIMITY) >= COMBO_MIN_SIGNALS:
            reasons.append("combined_drift")
        elif max(proximities) >= SOLO_PROXIMITY:
            reasons.append("near_boundary")

    return Verdict(alert=bool(reasons), pillar="checks", reason=",".join(reasons))


def check_contract_checkpoint(payload, ctx):
    r = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in r:
        return Verdict(alert=False, pillar="contracts", reason=r["error"])

    # contract_diff already computes real vs. declared schema/type violations
    # server-side — this is authoritative, not a threshold judgment call.
    reasons = list(r.get("violations") or [])

    freshness = r["freshness_delay_min"]
    f, _ = evaluate(ctx, "freshness_delay", freshness, hard_max=ctx.baseline["freshness_delay_max_min"])
    if f:
        reasons.append("sla_freshness")

    return Verdict(alert=bool(reasons), pillar="contracts", reason=",".join(reasons))


def check_lineage_run(payload, ctx):
    r = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in r:
        return Verdict(alert=False, pillar="lineage", reason=r["error"])

    b = ctx.baseline
    duration = r["duration_ms"]
    upstream_n = len(r.get("actual_upstream") or [])
    downstream_n = r["actual_downstream_count"]
    job = payload.get("job", "unknown")

    reasons = []
    f, _ = evaluate(ctx, f"duration:{job}", duration, hard_max=b["lineage_duration_ms_max"])
    if f:
        reasons.append("runtime_anomaly")

    # Learn typical edge counts for this job from the stream itself; only
    # trust the learned mode once it's had a few clean observations to settle.
    modes = ctx.state.setdefault("_modes", {})
    up_seen = len(modes.get(f"upstream_n:{job}", {}))
    down_seen = len(modes.get(f"downstream_n:{job}", {}))
    typical_up = _mode_track(ctx, f"upstream_n:{job}", upstream_n)
    typical_down = _mode_track(ctx, f"downstream_n:{job}", downstream_n)

    if up_seen >= 1 and upstream_n < typical_up:
        reasons.append("missing_upstream")
    if downstream_n == 0 or (down_seen >= 1 and downstream_n < typical_down):
        reasons.append("orphan_output")

    return Verdict(alert=bool(reasons), pillar="lineage", reason=",".join(reasons))


def check_feature_materialization(payload, ctx):
    r = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in r:
        return Verdict(alert=False, pillar="ai_infra", reason=r["error"])

    sigma = r["mean_shift_sigma"]
    f, _ = evaluate(ctx, f"mean_shift_sigma:{payload.get('feature_view')}", sigma,
                     hard_max=ctx.baseline["feature_mean_shift_sigma_max"], zscore=False)
    reasons = ["feature_skew"] if f else []

    return Verdict(alert=bool(reasons), pillar="ai_infra", reason=",".join(reasons))


def check_embedding_batch(payload, ctx):
    r = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in r:
        return Verdict(alert=False, pillar="ai_infra", reason=r["error"])

    b = ctx.baseline
    centroid_shift = r["centroid_shift"]
    doc_age = r["avg_doc_age_days"]

    reasons = []
    f, _ = evaluate(ctx, f"centroid_shift:{payload.get('corpus')}", centroid_shift,
                     hard_max=b["embedding_centroid_shift_max"])
    if f:
        reasons.append("embedding_drift")
    f, _ = evaluate(ctx, f"doc_age:{payload.get('corpus')}", doc_age,
                     hard_max=b["corpus_avg_doc_age_days_max"])
    if f:
        reasons.append("corpus_staleness")

    if not reasons:
        proximities = [
            _proximity(centroid_shift, b["embedding_centroid_shift_max"]),
            _proximity(doc_age, b["corpus_avg_doc_age_days_max"]),
        ]
        if sum(1 for p in proximities if p >= COMBO_PROXIMITY) >= COMBO_MIN_SIGNALS:
            reasons.append("combined_drift")
        elif max(proximities) >= SOLO_PROXIMITY:
            reasons.append("near_boundary")

    return Verdict(alert=bool(reasons), pillar="ai_infra", reason=",".join(reasons))
