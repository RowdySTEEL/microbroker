# Event-study methodology

## Scope

New modules calculate descriptive intraday simple returns from existing sealed analysis and same-stream receipt snapshots. They have no order, account, ledger or broker-write capability. The local R18 analyzer, abstention rules, R20/R21 strategy and R22–R29 execution safety rules are unchanged. No DDL or dependency additions. Versions: application 0.30.0, collection purpose R30, existing price snapshot format R14, existing analysis snapshot format R10 with R18 result content.

Commands: `verify-event-study`, `demo-event-study --output DIR`, `event-study` (explicit snapshot pair / Paper or research session UUID / latest eligible research audit), `run-event-study-session`. Text and JSON output; optional exclusive JSON export includes all event rows. No persistent event-study table: every report revalidates source digests/manifests and computes a source/policy/full-row study digest.

## Event selection without future label substitution

Load the entire frozen analysis revision history, not only the article state at the final cutoff. For each analysis availability timestamp, recompute that article's then-visible provider revision using the existing `state_at` ranking. Analyses completed after their revision was already superseded are excluded. First current analyzed revision per article/company in the price/event time window consumes that article/company identity even if its company decision is withheld. Later revisions cannot cherry-pick an accepted label or multiply that article's sample count. A future revision cannot erase a prior eligible event.

Only R18 guarded company decisions are supported in R30. Pending analysis, unsupported legacy format, missing/ambiguous/shared context and withheld/conflicting decisions are explicit exclusions. No document/raw label fallback. Accepted positive, negative and neutral decisions form separate groups. SPY is a benchmark, never a company event. These events are accepted research decisions, not necessarily decisions accepted by Paper trading gates: news-age/risk/trade policies are not applied.

The event interval starts at the receipt snapshot start; feature availability must be no later than `min(price_end, analysis_as_of)`. Later outcome prices may extend beyond the analysis snapshot cutoff, because they are outcomes rather than features. No later analysis may be backdated into the interval. News/analysis/receipt timestamps are recorded timestamps, not exact commit visibility.

## Reference and horizon definition

Given feature timestamp F in nanoseconds, reference bar start is `ceil(F / 60s) × 60s`. Its close at start+60s is the reference P0. Inclusive equality allows a bar starting exactly at F. +1/+5/+15/+60 horizons are measured from this reference completion: target bar start = reference start + H minutes. Feature-to-reference completion and receipt delays and feature-to-target completion are disclosed per row. These labels must never be described as returns from publication or exactly F+H.

This choice is a conservative full-minute reference proxy compatible with the existing simulation convention. It can miss an earlier news response; it does not represent an executable quote or a fill. No model of spread, slippage, fees, volume or short borrowing. It is deliberately not an optimization of the Paper execution rule.

Reference and target bar starts must fall in weekday 09:30 <= America/New_York time <16:00 on the same NY date. ZoneInfo handles standard/daylight-saving offsets. This is a clock-hours filter, not an exchange calendar: holidays and early closes are not inferred. The fixed reference never searches forward to the next open. A feature just before 09:30 can reference 09:30 if the exact following full bar is recorded.

## Receipt price selection and missing data

Input must be an Alpaca raw IEX/SIP 1Min R14 `same_stream_run_receipts` snapshot. Existing readers validate every record, receipt payload, ordinal, dataset digest and manifest. Additional nondecreasing receipt-time check fails on local clock reversal. Current corrected bars without receipts are rejected.

Only the first base `b` receipt for each symbol/minute is selected. All `u` updates and later base duplicates are excluded; no final correction is retrospectively used. First invalid base receipt is not rescued by a later duplicate. All selected receipts must fall at/before the study cutoff. Receipt before bar completion is invalid without tolerance. Receipt delay >120s is invalid; this is a disclosed research-data quality rule, not a change to trading thresholds. Reference must have been observed before the target completion; otherwise a short outcome is withheld.

Every minute from reference through target must be valid and present for the asset. The benchmark must independently satisfy the same condition. No interpolation, nearest-price substitution or carrying forward. A target beyond source cutoff is right-censored. Missing IEX minutes are missing observed slots; they do not prove a feed outage or absence of exchange trades. The continuity filter may select more liquid periods and its exclusion counts must be considered in any inference.

If SPY was never collected, retain any valid absolute asset returns and disclose missing benchmark; if SPY is partially missing/invalid, withhold only that outcome's comparison. No zero benchmark return fallback, different-minute match or cross-feed merge.

## Arithmetic and summary interpretation

`R_asset = (P1/P0 - 1) * 10000` bps, SPY likewise. Market-adjusted metric = R_asset - R_SPY (implicitly beta=1); no fitted market model or beta. Directional adjusted metric is the same for positive labels and sign reversed for negative labels; neutral has no direction. Negative directional figures are research statistics, not short-position P&L.

Decimal calculation uses a private 50-digit, half-even context, independent of caller Decimal settings. Public return strings use eight decimal places in bps. Group means/medians summarize these disclosed rounded per-row values, equally weighted by eligible article/company row, separately for each horizon. Empty groups retain n=0 and null statistics. Absolute n can exceed benchmark-matched n. `asset_on_benchmark_matched_rows` supplies the comparable asset subset; the full asset mean minus a smaller SPY mean must not be treated as the mean paired difference.

Stats are descriptive. Neutral cases are observed accepted neutral decisions, not random or causally matched controls. Random-time controls, event-type labeling, pre-news move filters, economic-event clustering across articles, beta estimation, standard errors/p-values, chronological holdout calibration, exit-policy tests and net profitability are not included. No claim of independent observations or signal edge. These are subsequent research steps; R30 supplies auditable outcomes and coverage first.

## Order-free collection

`EventStudySessionRequest` is distinct from the regular SessionRequest. Duration 60..10800s, default 5400s, poll default 30s within the existing 30..300 range. It normalizes company symbols against the configured universe, excludes SPY from news/analysis selection and automatically adds SPY to prices. A SPY-only company universe is rejected. The existing `run_session` uses optional `news_symbols` for news, analysis selection and frozen news; ordinary/Paper requests preserve their original symbols and 60..3600 bounds.

Collection uses existing market-data adapters and local FinBERT, separate stream connection ownership, per-stage commits and final snapshots. No Paper API constructed. Existing Paper positions are not monitored or liquidated by this research command. Session final JSON includes a next-command hint; offline reports resolve saved source UUIDs and verify stream/analyzer identity. The research audit pointer is not independently authenticated; underlying snapshot integrity is checked.

At 90 minutes, only roughly the first 28 minutes of event arrivals can have a full +60 horizon, depending on reference and receipt times. New arrivals near the end are censored, not failures. No restart/backfill/scheduler/supervisor is added. Existing bounds still apply: 10000 snapshot receipt/version records, 64 MiB source bounds, 2000 event/revision-company rows, <=100 displayed rows and 100 recent session lookup candidates. Limits fail explicitly rather than silently truncating calculations. Full export retains all bounded event rows; live collection/network is separate from offline analysis.
