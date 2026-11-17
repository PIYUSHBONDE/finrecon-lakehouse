# GitHub publication handover

Suggested repository name: `finrecon-lakehouse`.

Suggested description: "Local Spark/Delta payment reconciliation with PostgreSQL
CDC, auditable exceptions, and verified recovery."

Suggested topics: `data-engineering`, `pyspark`, `delta-lake`, `apache-kafka`,
`debezium`, `postgresql`, `financial-reconciliation`.

## Prepared local snapshot

Publication preparation creates a separate Git repository under
`pipelines/output/portfolio/`. It stages a curated source snapshot and the public
aggregate evidence. The active workspace, Docker volumes, and existing release
archive remain separate. The snapshot has no remote and no commits. No author
identity or license is invented, and no account/repository is created remotely.

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

No project license has been selected. Decide the publication license, account,
and visibility before publishing. The staged README describes the local edition
and distinguishes unimplemented managed/model integrations.

## Commit and publish after review

Configure your actual Git author identity if it is not already configured. The
following commands are a manual handover; preparation does not execute them:

```powershell
git commit -m "feat: add verified local reconciliation lakehouse"
```

Create the intended empty repository in your chosen GitHub account. Use its
actual URL rather than the placeholder below, then publish the reviewed commit:

```powershell
$repositoryUrl = 'https://github.com/YOUR_GITHUB_OWNER/finrecon-lakehouse.git'
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
