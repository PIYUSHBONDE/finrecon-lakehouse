# Local PostgreSQL CDC

Run from the repository root:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

PostgreSQL is exposed on host port `15432`; its container port remains `5432`.
This avoids the separate Windows PostgreSQL instance listening on host port
5432. Debezium's `database.hostname = postgres` and `database.port = 5432` are
container-network settings and do not change with the host mapping.

Wait until Kafka Connect responds at `http://localhost:8083/connectors`.
Starting the container does not register a connector automatically. Register or
update the checked-in configuration:

```powershell
$spec = Get-Content -Raw docker/debezium/register-postgres-connector.json | ConvertFrom-Json
$body = $spec.config | ConvertTo-Json -Depth 10
Invoke-RestMethod -Method Put -Uri "http://localhost:8083/connectors/$($spec.name)/config" -ContentType 'application/json' -Body $body
Invoke-RestMethod -Uri "http://localhost:8083/connectors/$($spec.name)/status" | ConvertTo-Json -Depth 6
```

Both the connector and task 0 must report `RUNNING`. Status can briefly be
unavailable while Kafka Connect starts the new connector.

## Configuration correction

The original JSON targeted a different PostgreSQL setup:

| Setting | Original | Correct for this Compose stack |
| --- | --- | --- |
| `database.user` | `postgres` | `finrecon` |
| `database.password` | `password123` | `finrecon` |
| `database.dbname` | `core_ledger` | `finrecon` |
| Topic namespace setting | `database.server.name` | `topic.prefix` |

Debezium 2.0 renamed `database.server.name` to `topic.prefix`; the configured
image uses 2.4. The prefix value remains `finrecon_db`, producing:

- `finrecon_db.public.accounts`
- `finrecon_db.public.invoices`
- `finrecon_db.public.ledger_entries`

These are local development credentials. `decimal.handling.mode = string`
preserves monetary precision in JSON; cast values into `DECIMAL(18,4)` in Spark.
The schema enables `REPLICA IDENTITY FULL` for before/after update values.

## Verified on 2026-11-10

- All three containers started successfully.
- PostgreSQL reported `wal_level = logical` and all three source tables existed.
- Connector configuration validation returned zero errors.
- Connector and task reported `RUNNING`.
- A labelled account/invoice fixture generated six consumed Kafka CDC events:
  account insert/update, invoice insert/update, and two ledger inserts.
- Update events preserved before/after values.
- The invoice amount remained the string `123.4567`.
- Debit and credit totals each equalled `123.4567` using decimal arithmetic.

Local evidence is under `data_generator/output/cdc_smoke_verification.json` and
`cdc_smoke_events.json`. Smoke fixture rows remain in PostgreSQL for inspection.
This verifies CDC connectivity; it does not implement a continuous ledger
workload generator or Bronze ingestion.

The producer now generates matching ledger data as documented in
[correlated ledger generation](../../data_generator/README.md#correlated-ledger-source).
Bronze ingestion remains separate work.

Property rename reference: [Debezium 2.0 release notes](https://debezium.io/blog/2022/10/17/debezium-2-0-final-released/).
