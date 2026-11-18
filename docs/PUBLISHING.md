# GitHub publication handover

Repository: [PIYUSHBONDE/finrecon-lakehouse](https://github.com/PIYUSHBONDE/finrecon-lakehouse).
The reviewed 94-file snapshot was published on November 17, 2026 as commit
[`40bae34`](https://github.com/PIYUSHBONDE/finrecon-lakehouse/commit/40bae34c468781c85f2180097b9646537b9fb01b).
See [CI.md](../pipelines/CI.md) for current runner operation and observed CI results.

Suggested repository name: `finrecon-lakehouse`.

Suggested description: "Local Spark/Delta payment reconciliation with PostgreSQL
CDC, auditable exceptions, and verified recovery."

Suggested topics: `data-engineering`, `pyspark`, `delta-lake`, `apache-kafka`,
`debezium`, `postgresql`, `financial-reconciliation`.

## Prepared local snapshot

Publication preparation creates a separate Git repository under
`pipelines/output/portfolio/`. It stages a curated source snapshot and the public
aggregate evidence. The active workspace, Docker volumes, and existing release
archive remain separate. The November 15 preparation initially had no remote or
commits. Publication later connected the owner-supplied repository and created
one real initial commit. No historical development commits were manufactured.

The adjacent `review.json` records the exact snapshot path, staged file count,
content hashes, and checks. Open that report to find the prepared repository.
Review it from its own directory:

```powershell
git status --short
git diff --cached --stat
git diff --cached --check
git diff --cached -- README.md docs
git remote -v
```

Included: code, contracts, Docker definitions, scripts, tests, architecture/demo
guides, and curated aggregate evidence. Excluded: virtual environments, caches,
SQLite outboxes, generated bank files, raw pipeline reports, physical backups,
release archives, and the active workspace's Git metadata. Local demonstration
credentials in the Compose/connector examples are intentionally visible; this
is not production credential configuration.

No project license file has been added. License selection remains the owner's
decision. The supplied repository is public. The README describes the local
edition and distinguishes unimplemented managed/model integrations.

## Commit and publish after review

Configure your actual Git author identity if it is not already configured. These
commands describe future reviewed changes; initial publication has completed:

```powershell
git commit -m "feat: add verified local reconciliation lakehouse"
```

For another newly prepared checkout, add its intended URL only if no remote
exists. The maintained publication checkout already has `origin`:

```powershell
$repositoryUrl = 'https://github.com/PIYUSHBONDE/finrecon-lakehouse.git'
git remote add origin $repositoryUrl
git push -u origin main
```

The initial commit represents this prepared snapshot. Do not manufacture a
historical series of commits for work that was not committed as it happened.
Subsequent changes should be made in the chosen maintained checkout and synced
deliberately; the snapshot does not update itself when the development workspace
changes.

## CI activation is separate

The workflow uses a dedicated `[self-hosted, windows, x64, finrecon]` runner and
has no hosted-runner fallback. A trusted `main` push can queue the workflow until
a matching runner is available. Do not describe a queued workflow as a passed
GitHub CI run. Use a separate runner checkout from the active development folder.
See [CI setup and measured local results](../pipelines/CI.md).

After publication, verify the README/diagram rendering, evidence links, repository
visibility/license, and the actual Actions result if a runner is activated.
Databricks deployment and paid model integrations are not activated by this step.
