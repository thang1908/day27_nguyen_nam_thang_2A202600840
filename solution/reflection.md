# Reflection (≤1 page)

**Which fault types were hardest to catch, and why?**

The subtle-tier instances of `feature_skew`, `embedding_drift`, and
`corpus_staleness` were the hardest by a wide margin. On the `practice` and
`public` streams, "obvious" tier faults sat so far past `ctx.baseline`'s
mean±3σ thresholds that a plain threshold check caught essentially all of
them (practice: TPR 1.0, FPR 0.0). The private stream's much heavier mix of
subtle-magnitude faults is where detection broke down: TPR fell to 0.648
while FPR stayed near zero (0.0068), meaning my defense is conservative
rather than sloppy — it just isn't sensitive enough to the faults that sit
close to normal variance.

I tried adding a self-calibrating layer (running mean/std per signal, kept
in `ctx.state`, only updated from points that weren't already flagged so a
run of faults can't drag the "normal" model toward itself) on top of the
static baseline. That measurably improved `public` (TPR 92.3% → 94.9%,
FPR barely moved) but produced *zero* TPR gain on `private` when re-run —
same misses, same count, just a hair more FPR. That's the clearest evidence
I have that tuning against public's coarse per-pillar bands doesn't
transfer to private's harder distribution, exactly as RULES.md warns.
Lineage (`missing_upstream`/`orphan_output`) was the one pillar where a
purely structural, non-threshold signal worked well end to end: learning
the typical upstream/downstream edge count per job from the stream itself
and flagging drops below it needed no magnitude tuning at all.

**What would you change about your cost/coverage tradeoff, if you had
another pass?**

Right now every handler spends exactly one metered call per event and
nothing more — full single-pass coverage, no follow-up calls, no unspent
budget held in reserve (practice: 180/220 spent; private: within its 320
budget with headroom). That's cost-safe but leaves no room to spend more on
events that look borderline. With another pass I'd hold back a small
reserve (tracked via the free `budget_remaining()` call) and spend it
selectively: e.g. re-checking `lineage_graph_slice` or `feature_drift` a
second time later in the run once more history has accumulated, for events
that were "close but not flagged" the first time around, rather than
treating every event as a single, isolated, one-shot decision. I'd also
want a genuinely different subtle-fault signal, not just a sharper
threshold on the same value the baseline already uses — e.g. tracking
higher-moment statistics (skew of the running distribution, not just
mean/std) since the subtle faults that got past me were, by construction,
too close to the existing signal's normal range to separate with a stricter
cutoff on that same signal.
