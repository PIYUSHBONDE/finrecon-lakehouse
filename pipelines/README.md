# Local Bronze ingestion

Bronze runs entirely on the local Docker engine. The runner uses Apache Spark
3.5.7 and Delta Lake 3.3.2, two local executor threads, a 1 GB driver heap, and a
3 GB container memory limit. It is an on-demand job, not another always-on service.
Initial setup downloads free Docker/Python/Maven packages; later runs reuse caches.

## Inputs and outputs

| Input | Append-only Delta table |
| --- | --- |
| Gateway Kafka topic(s) | `bronze_gateway_events` |
| Debezium accounts/invoices/journal topics | `bronze_cdc_ledger` |
| Bank landing CSV files | `bronze_bank_settlements` |

Kafka rows retain exact binary keys/values, readable payloads, headers, topic,
partition, offset, and Kafka timestamps. Invalid JSON, invalid currencies,
negative amounts, duplicate retries, and tombstones are not discarded.

Bank columns are explicitly strings, including monetary values and dates.
CSV quoting and multiline memos are supported. `raw_payload` contains the parsed
row as JSON, or the corrupt record text when supplied by Spark's parser; it is
not a byte-for-byte CSV archive. The original uniquely named landing files remain
the source archive. Ingestion considers only `*.csv`, ignoring partial `.csv.tmp` files.

This is a fixed-header CSV reader. Header schema changes fail validation rather
than enabling automatic schema evolution. Parser corruption is recorded when
Spark classifies a row as corrupt; this does not provide Auto Loader's rescued-data
semantics. Financial validity checks happen later in Silver.

All tables carry `source_system`, UTC ingestion timestamp, and `ingest_date`.
Delta paths are partitioned by `ingest_date` in `/lakehouse/bronze/<dataset>/`.
Checkpoints live under `/lakehouse/checkpoints/<dataset>/`. Both survive container
exit in the `bronze_data` named volume. Maven dependencies use `spark_ivy`.

## Verification

Create a successful fixture with `data_generator.verify_sources`, then run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_bronze.ps1
```

The script selects the latest successful `correlated_*/live/verification.json`.
To select a specific fixture or use fresh checkpoints:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_bronze.ps1 -SourceReport data_generator/output/correlated_b5a71edf/live/verification.json -Dataset demo_b5a71edf
```

Checks include:

- 105 raw gateway deliveries, with all five application retries retained and
  unique Kafka `(topic, partition, offset)` identities.
- 100 bank rows preserving exact decimal strings, two missing direct references,
  and invoice IDs inside the original memo text.
- The fixture's 400 correlated invoice/journal CDC events, alongside other raw
  CDC history. Total CDC rows can therefore exceed 400.
- The 65% original merchant skew and one invalid gateway original.
- Query restart without extra rows, persistent query IDs, and new run IDs.
- A second fresh Docker process resuming the same checkpoint without replaying
  already ingested offsets or files.

The JSON report is in `pipelines/output/<dataset>/verification.json`. A report
with `previous_process_checkpoint_verified = true` confirms the second process
check passed. Reports, table data, and checkpoints are local; no paid resource
is created.

Run this verification against a quiet fixture: concurrent source writes can
legitimately change row counts between restart checks.

Verified on 2026-11-11 with dataset `portfolio_demo`:

| Check | Result |
| --- | --- |
| Gateway Delta rows | 105 |
| Raw CDC Delta rows, including prior fixture history | 1026 |
| Correlated invoice/journal CDC events | 400 |
| Bank Delta rows | 100 |
| Additional rows after query/process restart | 0 |
| Fresh-process checkpoint verification | Passed |

All four Bronze tests passed in the Spark container, including the CSV
precision/quoting/multiline test and arrival of a new file after restart.

## Run ingestion directly

```powershell
docker compose -f docker/docker-compose.yml --profile compute build bronze
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.bronze --gateway-topics gateway_events --bank-dir /workspace/data_generator/output/live/bank_settlements --dataset local
```

Use a source topic and bank directory that exist. Kafka addresses inside Docker
use `kafka:29092`, not the host's `localhost:9092` listener. `--max-offsets-per-trigger`
defaults to 10000 and `--max-files-per-trigger` to 25. Streams run sequentially
to limit memory pressure. `AvailableNow` processes available work, then exits;
later arrivals need the next invocation.

Source manifests bind each dataset to its broker, topic lists, input directory,
and table/checkpoint root. Rate limits can change. Choose a new dataset for a
different source configuration. Run only one job for a dataset at a time.
Never delete checkpoints or use `docker compose down -v` as a restart mechanism:
those actions remove recovery state, and replay into an existing table can add
duplicates. Kafka retention loss raises an error instead of silently skipping data.

## Tests

Configuration tests run with standard Python. Real Spark tests run in the runner:

```powershell
python -m unittest discover -s tests -p test_bronze.py -v
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_bronze.py -v
```

References: [Spark Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html),
[Spark CSV options](https://spark.apache.org/docs/3.5.7/sql-data-sources-csv.html),
and [Delta/Spark version compatibility](https://docs.delta.io/releases/).
