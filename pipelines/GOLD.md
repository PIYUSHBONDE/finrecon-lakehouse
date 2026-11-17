# Gold reconciliation and persistent open items

Update: the [exception stage](EXCEPTIONS.md) can now supply validated reference
overlays. The original 98-match baseline below remains historical evidence.
After both fixture memo links validate, the fixture has 100 matches and no
fixture open items. Unrelated historical test items remain visible.

Run after successful gateway/bank Silver and CDC jobs:

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.gold --dataset portfolio_demo
```

This stage uses the existing local Spark/Delta image and persistent lakehouse
volume. It adds no service, cloud account, or paid API. Run one writer per dataset
and serialize Silver/CDC publication with Gold for a consistent business cut.
Each source table version is pinned; the five source snapshots are not a
cross-table transaction.

## Exact matching policy

An invoice is `MATCHED` only when all checks pass:

- One active invoice in `PAID` state and exactly one gateway event for that invoice.
- A nonempty gateway reference identifying exactly one gateway event globally.
- Exactly one bank settlement with that gateway reference.
- Equal invoice, gateway, bank, and summed debit/credit amounts.
- Equal invoice/gateway/bank currencies and consistent account/gateway/bank merchants.
- At least two active journal legs, equal total debits and credits, and active
  source accounts. Positive debits belong to the invoice's payment account.

Ambiguous references, duplicate settlement links, missing evidence, deleted or
inactive accounts, and mismatched financial values remain `OPEN` with reasons.
There is no random winner, tolerance, amount-only match, or currency conversion.
Every reported amount uses `DECIMAL(18,4)`. Spark widens decimal sums internally;
the final checked cast rejects totals outside `DECIMAL(18,4)` without rounding
away a scale difference. No floating-point arithmetic is used.

Journal currency comes from its invoice foreign key. The current account schema
has no currency column, so this stage cannot independently validate an account's
currency designation. Account-currency modeling requires a source schema change.

Bank records without a gateway link become separate bank open items, preserving
their raw remittance memo. A memo containing an invoice-looking token does not
automatically prove the relationship. Extraction and validated link proposals
belong to the separate exception stage. An invoice missing such a bank record and
the orphan bank are two unresolved sides, not two additional payments.

## Incremental evidence and indexes

The first run enables Delta Change Data Feed on the five Silver inputs and reads
one bootstrap snapshot of each. Later runs read CDF between the saved and pinned
versions. Updates use preimages to invalidate old links and postimages to update
current evidence. Only current SCD markers enter the evidence cache.
Duplicate current source keys within the same commit or bootstrap snapshot fail
the job before evidence writes; the job never chooses an arbitrary source row.

`state_reconciliation_inputs` stores current invoice, gateway, bank, journal,
and account evidence. It also stores gateway-reference and account reverse
indexes. It partitions by evidence kind and 32 hash buckets of the lookup key.
Queries prune these buckets before filtering exact keys. No transaction key
collection on the driver is required; only the at-most-32 bucket numbers are
collected for each lookup. Delta metadata still enumerates table files.

Changed evidence and existing open items identify invoices to reconsider.
Account changes can reopen previously matched invoices through the reverse
index. A late bank can resolve an invoice from a previous day without scanning
old invoice history. When no Silver version changes, Gold returns immediately
and retains its open-item state without changing timestamps.

Large fact joins use invoice or gateway-reference keys, not the merchant ID.
This spreads the 65% institutional-merchant traffic across transaction keys and
hash buckets. AQE and skew-join handling are explicitly enabled. This is a
correctness implementation on one host. Separate [synthetic skew experiments](BENCHMARKS.md)
compare shuffle joins, AQE, salting, and invoice-key controls; they do not prove
large-scale end-to-end Gold throughput. Dated reporting conversion now has a separate
[FX policy and broadcast join](FX.md); native reconciliation still requires the
same currency across its sources.

## Outputs and recovery

All tables live under `/lakehouse/gold/<dataset>/`:

| Table | Purpose |
| --- | --- |
| `gold_reconciliation` | Latest invoice decisions plus unresolved orphan banks |
| `state_unreconciled_open_items` | Only current `OPEN` decisions, carried across runs |
| `state_reconciliation_inputs` | Bucketed evidence and reverse indexes |
| `state_reconciliation_cursors` | Source table identities and acknowledged Delta versions |

Matched invoices leave the open-item table. A later CDC change may reopen them.
Resolved orphan bank rows leave both decision and open-item tables because their
evidence now belongs to an invoice decision. Stable item IDs and content hashes
make retries idempotent. `first_seen_at` is stable while a row remains present;
`last_changed_at` advances only when its decision content changes. A reopened
open-item row starts a new open episode; the invoice decision retains its
original first-seen time. These tables are current state, not an immutable
business decision event log; Delta table history supplies retained snapshots.

Evidence, decisions, open items, and cursors are separate Delta transactions.
Cursor acknowledgement is last. A failure after a decision commit can temporarily
leave open items stale; consumers must wait for job success. Retry reapplies the
same CDF range and repairs state without duplicate decisions or timestamp churn.
There is no claim of multi-table atomic publication.

Never replace tracked Silver tables, disable CDF, or vacuum away unconsumed CDF.
Source identity changes or missing CDF cause the job to fail. It does not silently
skip lost history or guess a new cursor. Such changes require a deliberate
rebuild from validated source data. Preserve the local Delta volume.

## Verification

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_gold.ps1
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_gold.py -v
```

The fixture verifier requires successful Bronze, gateway/bank Silver, and CDC
proofs. It checks 100 correlated invoices: 98 exact three-way matches, two
invoices awaiting bank links, and two orphan bank memos. The shared dataset also
contains older CDC test fixtures; their unmatched invoices remain visible and
are counted separately in the total. Sorted table digests must stay identical
after a no-change run and a second fresh container. Results are saved to
`pipelines/output/<dataset>/gold_verification.json`.

Verified locally on 2026-11-12: 98 matches from 100 correlated invoices, two open
invoice sides, and two memo exceptions. The shared fixture contains 213 current
decision rows and 115 open items, including older test data. All four table
digests remained identical after a no-change run and a second fresh container.
Three focused Spark/Delta tests passed: reconciliation/recovery, financial
failure cases with bucket pruning, and the independent source-key safety guard.

Focused tests cover exact `0.0001` amounts, amount/currency/merchant mismatch,
unbalanced journals, duplicate bank links, missing memo references, partition
filters, delayed bank arrival, failure before cursor acknowledgement, retry
stability, inactive accounts, deleted journal legs, decimal aggregate overflow,
shared gateway references, and disabling tracked CDF. Large-scale throughput,
multi-host failures, and expired-CDF recovery are outside this proof.

Reference: [Delta Lake Change Data Feed](https://docs.delta.io/delta-change-data-feed/).
