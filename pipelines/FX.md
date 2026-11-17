# Dated FX reporting

This stage values already matched payments in INR. Native reconciliation still
requires the same currency across gateway, invoice, and bank. Currency-mismatched
payments remain open. FX reporting changes neither native decisions nor sources.

The demo book, `data_generator/fixtures/fx_synthetic_20261111.csv`, contains
**synthetic test quotes, not market exchange rates**. It covers the existing
2026-11-11 payment fixture. No provider, download, or paid service is required.
Another business date or actual financial use requires an explicit rate book.

## Policy and contracts

Policy `inr_gateway_date_half_even_4_v1` uses the gateway payment's UTC
`event_date`. A quote means INR per one unit of native currency for that exact
date. Supported currencies are INR, USD, EUR, GBP, and JPY. Each book has one row
per date/native/reporting pair, with INR as its reporting target. Self quotes
are explicit rows with rate exactly `1.0000`.

Native amounts, rates, and converted amounts use `DECIMAL(18,4)`. Multiplication
uses widened decimal arithmetic. Round each payment once to four places with
HALF_EVEN, then check representability in `DECIMAL(18,4)`. A small positive amount
can round to zero. The total sums individually rounded conversions and must also
fit `DECIMAL(18,4)`. Rounding follows Spark's documented
[HALF_EVEN bround](https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.functions.bround.html).

Missing dates/quotes, row overflow, and total overflow block report success.
There is no prior-day fallback, inverse quote, float calculation, or currency
matching tolerance. Rates with more than four fractional places are rejected;
adapting a higher-precision provider requires an explicit contract/policy change.

The CSV header is `rate_date,currency,reporting_currency,rate,rate_id,provenance`.
Dates use `yyyy-MM-dd`; rates are positive decimal strings. Quote IDs and
provenance are required. Rules live in `contracts/fx.yaml`.

Books are limited to 10 MiB and 10,000 rows before broadcast. Exact bytes are
archived at `/lakehouse/fx_books/<dataset>/<SHA256>.csv`. The hash is the immutable
book ID. Valid rows enter `silver_fx_rates`, partitioned by book ID. Replay adds
no duplicate pairs. A new file is a separate book release; it preserves earlier
quotes. Each invocation selects exactly its supplied book without mixing releases.

Invalid parsed rows enter `silver_fx_rate_quarantine` with raw parsed payload,
failed rules, book ID, and contract fingerprint. The whole invalid book is
rejected before its rates are admitted. Duplicate dated pairs reject the whole
book, including identical duplicates. Incompatible CSV headers fail parsing.
Correct the input and submit a new book; original bytes remain archived.

## Output and operation

The rate join explicitly broadcasts the validated book. Tests check the executed
plan for `BroadcastHashJoin`. This proves the local join strategy, not enterprise
throughput. Batch reporting scans current matched decisions each run.

`gold_fx_valuation` contains native/reporting values, business date, quote ID and
provenance, book ID, policy, reconciliation hash, valuation status, and change
timestamp. Gold and gateway Delta snapshots are pinned and recorded in the run
report. Serialize upstream publication and use one writer per dataset; these
snapshots and the rate/output tables are not one cross-table transaction.

A single MERGE inserts/updates changed valuations and removes payments that are
no longer matched. Unchanged replay preserves row content and timestamps. Missing
evidence or overflow yields a failure status; a failed run reports no complete
converted total. An invalid book may leave the previous output snapshot in place.
Consumers must check the whole run status before using its valuation output.

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.fx --dataset portfolio_demo --rates-file /workspace/data_generator/fixtures/fx_synthetic_20261111.csv
```

The default report is `pipelines/output/<dataset>/fx_run.json`. Add
`-FxRatesFile /workspace/data_generator/fixtures/fx_synthetic_20261111.csv` to
the existing `scripts/run_pipeline.ps1` command to append an eighth stage after
Gold quality. Omitting the option retains the seven-stage pipeline. No synthetic
rate book is selected automatically. Deterministic FX rejection returns exit 2
and fails the orchestrated run. Source decisions remain unchanged.

## Verification

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_fx.ps1
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_fx.py -v
```

The verifier checks all 100 fixture conversions against an independent Python
Decimal oracle with precision 50 and HALF_EVEN rounding. It checks exact types,
preserved reconciliation/bank data, and stable valuation digests after replay
and a second fresh container. Results: `pipelines/output/<dataset>/fx_verification.json`.

Tests cover tie rounding, maximum exact amounts, missing currency/date, no date
fallback, row and aggregate overflow, contract quarantine, duplicate pairs,
immutable revisions, broadcast execution, replay, and removal of reopened items.
Cross-currency settlement reconciliation, account currency modeling, provider
integration, FX gains/loss postings, fees, and scale benchmarks remain future work.

Verified locally on 2026-11-12 with all 100 fixture payments and five synthetic
quotes. The synthetic INR total was `13075239.0763`; it is not a market valuation.
All three focused tests passed, including the physical broadcast plan and total
overflow check. Replay in two fresh containers changed zero rows, and both
native reconciliation and bank data remained unchanged.

The Windows wrapper also completed all eight stages with the demo book explicitly
selected. Integrated report:
`pipelines/output/portfolio_demo/runs/20261112T043853Z_39db1599/run.json`.
