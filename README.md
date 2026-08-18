# bottle-backup

User-controlled incremental backups and cross-instance migration for Cloud in a Bottle, powered by [restic](https://restic.net).

## What it does

This app backs up the persistent data on a Cloud in a Bottle instance to a storage backend you control: S3, Backblaze B2, SFTP, Google Cloud Storage, Azure Blob, OpenStack Swift, rclone remotes, a restic REST server, or a local directory. Backups are incremental and deduplicated (restic handles this automatically), so only changed data is transferred on each run. It also provides a migration tool that pushes apps and data from one Cloud in a Bottle instance to another over HTTP.

The app has `access_all_data = true` in its manifest, which means it can see and back up every app's persistent data directory, temp data, and VM-level data. The archive tier (`/data/app_archive`) is intentionally excluded because it already lives in durable storage (an S3 bucket or host-managed local archive).

## Getting started

1. Install the app from the Cloud in a Bottle dashboard (point it at this repo).
2. Open the Backup UI at `https://backup.<your-zone>/`.
3. Enter your restic repository URL and password.
4. Optionally configure backend credentials (AWS keys, B2 keys, etc.) in the environment variables section.
5. Click "Test" to verify access.
6. Click "Run backup now" or set an automatic interval.

## Backup scope

Each backup captures these directories (if they exist on the instance):

| Path | Contents |
|------|----------|
| `/data/app_data` | Persistent app data (databases, config, user files) |
| `/data/app_temp_data` | App temp data (caches, build artifacts) |
| `/data/vm_data` | VM-level data (router database, SSH keys) |

Excluded from backups:

| Path | Reason |
|------|--------|
| `/data/app_data/backup` | The backup app's own restic repo (self-inclusion would grow unboundedly) |
| `/data/app_archive` | Archive tier is its own durable store; double-storing through restic would inflate snapshots without adding safety |

## Supported backends

Any backend restic supports works here. The repository URL format follows restic's conventions:

| Backend | URL format | Example |
|---------|-----------|---------|
| Amazon S3 | `s3:s3.amazonaws.com/bucket` | `s3:s3.us-east-1.amazonaws.com/my-backups` |
| Backblaze B2 | `b2:bucket-name:path` | `b2:my-backups:/bottle` |
| SFTP | `sftp:user@host:/path` | `sftp:backup@nas.local:/backups` |
| Google Cloud Storage | `gs:bucket:/path` | `gs:my-backups:/bottle` |
| Azure Blob | `azure:container:path` | `azure:backups:/bottle` |
| OpenStack Swift | `swift:container:/path` | `swift:my-backups:/bottle` |
| REST server | `rest:http://host:port/` | `rest:https://restic.example.com/` |
| rclone | `rclone:remote:path` | `rclone:b2-remote:backups/bottle` |
| Local path | `/path/to/repo` | `/data/app_data/backup/local-repo` |

Backend credentials (like `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for S3) are set through the environment variables field in the UI. The UI includes inline examples for common setups like S3.

A local-path repo stores backups on the same disk as the instance, which provides no protection against disk failure. The UI warns about this. Use a remote backend for real disaster recovery.

## Automatic backups

Set an interval (in seconds) in the configuration. The scheduler runs in the background and triggers a backup at each interval. The minimum interval is 60 seconds. On startup, the scheduler checks when the last backup ran and waits only the remaining time before the next one, so restarting the app does not reset the countdown.

Set the interval to 0 (or leave it blank) to disable automatic backups.

## Retention (expiring old backups)

After each successful backup, old snapshots are expired according to a retention policy you set in the UI (the **Retention policy** section of the configuration). It maps directly onto restic's [`forget`](https://restic.readthedocs.io/en/stable/060_forget.html) keep-* rules:

| Field | restic flag | Meaning |
|-------|-------------|---------|
| Keep last | `--keep-last` | The N most recent snapshots, regardless of time |
| Keep hourly | `--keep-hourly` | The newest snapshot from each of the last N hours that have one |
| Keep daily | `--keep-daily` | The newest snapshot from each of the last N days that have one |
| Keep weekly | `--keep-weekly` | The newest snapshot from each of the last N weeks that have one |
| Keep monthly | `--keep-monthly` | The newest snapshot from each of the last N months that have one |
| Keep yearly | `--keep-yearly` | The newest snapshot from each of the last N years that have one |

A snapshot is kept if it matches **any** rule (the rules are OR'd), so tiers combine additively — e.g. `keep_last=5, keep_daily=7, keep_weekly=4` keeps the 5 most recent plus one per day for 7 days plus one per week for 4 weeks, deduplicated where they overlap. Set a field to 0 to disable that tier. **If every field is 0, nothing is expired** — the safety floor means the app never issues a `forget` with zero keep rules (which would delete everything).

The policy is applied across all `bottle`-tagged snapshots (and legacy `openhost`-tagged ones) as a single group (`--group-by ''`), which assumes one instance per repository. Every snapshot is recorded with a stable host (`--host`, set to the zone domain) so its identity doesn't change when the backup container is redeployed.

Retention runs `forget` inline after the backup (fast — it only rewrites metadata) and reconciles the backup history database to match. The actual space is reclaimed by a `restic prune`, which runs **in the background** and only when snapshots were actually expired, so it never delays the backup or blocks the UI.

## Snapshots

Each successful backup creates a restic snapshot tagged with `bottle`. Older snapshots tagged `openhost` are still listed, restored, and expired. The UI lists snapshots newest-first and lets you:

- Browse files in any snapshot (organized by data root: app_data, app_temp_data, vm_data)
- Restore a snapshot (to all data roots, or to a specific root)
- Delete a snapshot (runs `restic forget --prune` to reclaim space)
- Name or rename a snapshot for easier identification

The Status panel shows the **repo size** — the deduplicated, compressed on-disk footprint (`restic stats --mode raw-data`). Because computing it is slow on large/remote repos, the value is cached: it is recomputed and stored after each backup and after a snapshot delete/prune, and served from the cache on page load so it never blocks (or times out) a request. The backup history database is reconciled against restic on every snapshot listing, so rows for snapshots that no longer exist (expired by retention, or removed out of band) are cleaned up automatically.

## Restoring

Restore overwrites files in place. During a full restore (all data roots), the app excludes its own data directory and the archive tier, so a restore will not clobber the backup configuration or the restic repository itself. When restoring a single root, restic's `--include` filter is used instead, and the excludes do not apply (restic does not allow combining `--include` and `--exclude` in one restore command).

You can restore a full snapshot (all data roots) or a single root (for example, only app_data). During restore, a mutual-exclusion lock prevents concurrent backups or migrations.

After restoring, you will likely need to reload the affected apps through the Cloud in a Bottle dashboard so they pick up the restored data.

## Integrity checks

The "Run restic check" button runs `restic check`, which verifies the internal consistency of the repository (pack files, index, snapshots). The result and output are shown in the UI. This does not verify individual file contents against their original hashes, only that the repository structure is intact.

## Migration

The Migrate tab provides a one-click way to move apps and data from this instance to another Cloud in a Bottle instance. Both instances must have the backup app installed.

### How migration works

1. The source gathers metadata about installed apps (from the router database or API).
2. A manifest is sent to the target, which stops its apps and clears data directories for the apps being migrated.
3. Each app's data directory is compressed as a tar.gz archive and streamed to the target. Archives larger than 14 MB are split into chunks to stay under the Cloud in a Bottle reverse proxy's body size limit.
4. The target extracts received data, fixes file ownership, then deploys or reloads each app.
5. Apps that were running on the source are started on the target. Apps that were stopped remain stopped. Non-migrated apps that were stopped for the transfer are restarted.

### Migration requirements

- A router API token on the source instance (set via the config API or through the UI). The token is needed to list and stop apps during migration.
- An API token for the target instance (entered in the Migrate tab).
- The backup app must be installed and running on both instances.

The migration protocol version is 3. Path traversal is blocked in all tar extraction steps via a filter that rejects `..` segments and absolute paths.

## Configuration

Configuration is stored in `/data/app_data/backup/config.json` with permissions restricted to 0600. The config file holds:

| Field | Description |
|-------|-------------|
| `repo` | Restic repository URL |
| `repo_password` | Restic repository encryption password |
| `env` | Backend credential environment variables (e.g., AWS keys) |
| `interval_seconds` | Automatic backup interval (0 = disabled) |
| `keep_last` / `keep_hourly` / `keep_daily` / `keep_weekly` / `keep_monthly` / `keep_yearly` | Retention policy tiers (0 = tier disabled; all 0 = keep everything) |
| `router_api_token` | Cloud in a Bottle router API token (needed for migration) |

The config API (`POST /api/config`) requires a valid Bearer token to rotate the `router_api_token` after it has been set, preventing co-located containers from silently replacing it.

## API

All routes are registered at both `/path` and `/backup/path` to handle the Cloud in a Bottle base-path proxy.

### Backup and restore

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Web UI |
| GET | `/api/status` | Current status (running, last backup, interval, backend type) |
| GET | `/api/config` | Current configuration |
| POST | `/api/config` | Update configuration |
| POST | `/api/backup` | Trigger a backup (accepts optional `name` in JSON body) |
| GET | `/api/snapshots` | List all snapshots |
| GET | `/api/repo/stats` | Repository size and compression stats |
| POST | `/api/repo/test` | Test restic connection (accepts optional repo/password overrides) |
| POST | `/api/restore` | Restore a snapshot (JSON: `snapshot`, optional `root`) |
| GET | `/api/restore/status` | Restore progress |
| GET | `/api/snapshot/files` | Browse files in a snapshot (query: `snapshot`, `root`, `path`) |
| POST | `/api/snapshot/delete` | Delete a snapshot |
| POST | `/api/check` | Run `restic check` |
| GET | `/api/check/status` | Last check result |
| GET | `/api/history` | Backup history (query: `limit`, `offset`) |
| POST | `/api/backup/rename` | Rename a backup record |
| GET | `/health` | Health check (returns `ok`) |

### Migration

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/migration/status` | Migration progress and log |
| POST | `/api/migration/push` | Start a direct-push migration to another instance |
| GET | `/api/apps-status` | List apps via the local router API |
| POST | `/api/stop-all-apps` | Stop all non-backup apps |
| POST | `/api/chown-app-data` | Fix ownership on app_data (skips subuid-mapped files) |

### Migration receive endpoints (called by source instance)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/migration/receive/start` | Accept manifest, stop apps, clear data |
| POST | `/api/migration/receive/app/<name>` | Receive a single app's tar.gz |
| POST | `/api/migration/receive/chunk/<name>` | Receive a chunk of a large app's tar.gz |
| POST | `/api/migration/receive/data` | Receive all app data as one tar.gz (streamed to disk) |
| POST | `/api/migration/receive/finalize` | Deploy/restart apps after data transfer |

## Files

| File | Description |
|------|-------------|
| `app.py` | Quart web application: routes, restic wrappers, scheduler, config management |
| `operations.py` | Mutual-exclusion lock ensuring only one backup, restore, migration, or prune runs at a time |
| `migration.py` | Cross-instance migration logic (direct push protocol, tar streaming, app deployment) |
| `Dockerfile` | Python 3.12 Alpine image with restic and uv |
| `openhost.toml` | App manifest (2048 MB memory, 1000 millicores CPU, `access_all_data = true`) |
| `templates/index.html` | Single-page web UI with Backups and Migrate tabs |
| `tests/` | Pytest test suite covering routes, exclude logic, and migration |

## Data

All persistent state lives in `$OPENHOST_APP_DATA_DIR` (defaults to `/data/app_data/backup/`):

```
/data/app_data/backup/
  config.json      # Restic repo URL, password, backend credentials, schedule
  backups.db       # SQLite database tracking backup history
  restic-repo/     # Default local restic repository (only used for local backends)
```

## Concurrency and timeouts

Only one destructive operation (backup, restore, migration, or the background retention prune) can run at a time. The `OperationLock` in `operations.py` enforces this. `restic check` is also serialized against these operations since it acquires a repository lock. The background prune waits for the lock before running, so it never collides with an in-progress backup or restore.

Timeouts for restic operations:

| Operation | Timeout |
|-----------|---------|
| Backup | 6 hours |
| Restore | 12 hours |
| Check | 2 hours |
| Connection test | 10 seconds |
| Retention forget | 10 minutes |
| Prune | 6 hours |
| Snapshot forget/prune (manual delete) | 30 minutes |

If a restic process exceeds its timeout, it is killed and the operation is marked as failed.

Read-only commands (`snapshots`, `stats`, `ls`, `cat config`) run with `--no-lock` so concurrent page loads don't contend on the repository lock or leave a stale lock behind if a request is aborted. Lock-taking commands (`backup`, `restore`, `check`, `forget`, `prune`) run with `--retry-lock 1m` so that if another operation is briefly holding the lock, restic waits and retries for up to a minute instead of failing immediately with "repository is already locked". (Both flags require restic ≥ 0.16.)

Every restic invocation is logged (the command on start, exit code and elapsed time on completion), visible via `oh app logs backup`.

## Running tests

```
uv venv
uv pip install -r pyproject.toml
uv pip install pytest pytest-asyncio
uv run pytest tests/ -v
```
