# Local memo exception extraction

The exception stage targets only bank open items with
`MISSING_GATEWAY_REFERENCE`. It uses a native Spark regular expression for
complete `INV-<UUID>` tokens, with token boundaries and distinct candidate
counting. Repeated mentions of one token are one candidate. Zero tokens,
multiple distinct tokens, or an unknown invoice remain unresolved. Memo text
is processed as data, never as instructions or executable content.

This is a deterministic local baseline, not an LLM or embedding implementation.
It requires no model download, external request, or paid API. Unsupported memo
formats remain open. A future local model may propose additional candidates;
the financial validation below remains mandatory.

## Validation before applying a link

The parser proposes an invoice identity, not an amount or payment instruction.
The job builds a temporary bank-reference view and runs the existing Gold
matching engine against it. Validation requires a current paid invoice, one
gateway, a globally unique reference, matching merchant and currency, exact
`DECIMAL(18,4)` amounts, active accounts, and balanced journal legs. Existing
direct settlements and competing proposals participate in the same bank count.
They cannot be silently displaced by a memo proposal.

The job inserts a record into `exception_extraction_audit` containing the
original memo, raw record identity, bank evidence hash, candidate IDs, method
version, proposed gateway key/reference, decision, and rejection reasons.
`VALIDATED` means the proposal passed validation at that evaluation, not that
Gold publication has completed. Audit writes are insert-only and use a stable
proposal ID that includes candidate links and validation reasons. A later
successful evaluation can therefore append a new decision even if bank evidence
is unchanged. Rejected proposals remain available for review; no amount changes.

Validated proposals enter `state_exception_links`, keyed by settlement ID.
These are reference overlays. Silver bank records, Bronze, and remittance memos
remain unchanged. Gold's evidence cache represents overlays as `bank_link` rows.
In this row kind only, `account_id` carries the bound gateway business key;
`source_status` carries the original bank evidence hash. They are internal
overlay fields, not modifications to operational accounts or bank source data.

Every application rechecks the exact bank hash, raw record identity, gateway
key, invoice identity, amount, merchant, and currency. A bank update invalidates
an old binding. An inactive/deleted account or journal change reopens an invoice
through the existing Gold checks. A conflicting new settlement produces an
ambiguous bank count. Previously closed invoices are reconsidered through the
reverse link lookup when their original bank evidence changes.

Gold consumes link-table CDF alongside Silver changes. A new overlay triggers
reconciliation even when no source payment changes. A valid linked bank joins
its invoice decision and leaves the orphan-bank table. Invalidated bindings can
reintroduce the bank orphan and reopen the invoice. A retained audit decision
does not override current financial evidence.

Audit, link, and Gold tables use separate transactions. A failure after a link
commit can leave reconciliation pending. Retry consumes that link through CDF
and completes Gold without duplicating audit records or bank usage. Read the
whole pipeline result before consuming decisions. Use one writer per dataset;
the orchestrator holds its dataset lock across this job. Individual stage CLIs
must be serialized with source publication and other writers.

## Running and verifying

The normal orchestrator now runs seven stages: Bronze, Silver, CDC, Silver
quality, Gold, exceptions with Gold refresh, and Gold quality. Run only this
stage against an existing dataset with:

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.exceptions --dataset portfolio_demo
powershell -ExecutionPolicy Bypass -File scripts/verify_exceptions.ps1
```

The standalone extraction command refreshes Gold before and after proposing
links. It saves `pipelines/output/<dataset>/exception_extraction.json` by default.
The orchestrator places this report in its unique run directory. Extraction
errors fail the stage and block final quality/publication success.

The original 100-payment fixture proof checks two validated links, 100 matched payments, preserved
raw bank references/memos, no duplicate audit IDs, stable replay digests, and
Gold quality. Two fresh containers prove persistence. Existing historical test
data still contributes 111 open items; these are outside the 100-payment fixture.
The verifier now derives counts and invoice identities from its canonical fixture,
so the November 14 readiness cohort verifies twenty links and 1,000 matched
payments while retaining 311 unrelated historical open items. These are known
synthetic fixtures, not production row-count SLAs.

`scripts/verify_gold.ps1` supports the baseline and validated-link states. Its
proof format changes to version 2 for overlay support. A baseline transition
requires a new first proof; only a second comparable fresh-process digest
comparison sets `previous_process_state_verified` true.

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_exceptions.py -v
```

Focused tests cover token boundaries, repeated/ambiguous candidates, literal
instructions, exact financial rejection, link replay before Gold publication,
preserved raw records, and re-opening after a bank change. Large-scale extraction
performance and semantic interpretation of arbitrary prose remain unproven.

Verified locally on 2026-11-12: both memo gaps resolved, all 100 fixture payments
matched, raw bank records remained unchanged, and replay changed zero rows in
two fresh containers. Both exception tests and all four Gold regression tests
passed, including stale bank hashes and ambiguous references. Reports are in
`pipelines/output/portfolio_demo/exception_verification.json` and
`pipelines/output/portfolio_demo/gold_verification.json`.
