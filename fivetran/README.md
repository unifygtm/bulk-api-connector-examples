# Unify Bulk API — Fivetran custom connector

A [Fivetran Connector SDK](https://fivetran.com/docs/connectors/connector-sdk)
connector that syncs data out of [Unify](https://unifygtm.com) using its
asynchronous [Bulk API](https://docs.unifygtm.com/developers/guides/request-data/bulk-api).

The Bulk API is built for exporting large datasets without long-lived HTTP
requests. For each resource the connector:

1. **Creates a query job** (`POST {base}/query-jobs`), scoped to records changed
   since the last sync.
2. **Polls the job** (`GET {base}/query-jobs/{job_id}`) with backoff until it is
   `FINISHED`.
3. **Pages through results** (`GET {base}/query-jobs/{job_id}/results`) and
   upserts each row.
4. **Checkpoints a cursor** so the next sync only requests new or changed data.

## Tables

| Table                      | Bulk resource                       | Cursor field |
| -------------------------- | ----------------------------------- | ------------ |
| `company`                  | `/data/v1/objects/company`          | `updated_at` |
| `person`                   | `/data/v1/objects/person`           | `updated_at` |
| `event`                    | `/data/v1/events`                   | `created_at` |
| `sequence_enrollment`      | `/sequences/v1/enrollments`         | `updated_at` |
| `sequence_enrollment_step` | `/sequences/v1/enrollment-steps`    | `updated_at` |
| `task`                     | `/tasks/v1/tasks`                   | `updated_at` |

Each table is keyed on `id`. Object-record results are sorted ascending by
`updated_at`, so the connector checkpoints page by page. Event, sequence, and
task results aren't guaranteed to be cursor-ordered, so those checkpoint once
per job using the maximum cursor value observed. Because upserts are keyed on stable
record IDs, the small overlap an incremental `gte` filter produces is idempotent.

## Project layout

| File                 | Purpose                                                           |
| -------------------- | ----------------------------------------------------------------- |
| `connector.py`       | `schema()` + `update()` — the Fivetran entry points.              |
| `unify_bulk.py`      | `UnifyBulkClient` — query-job lifecycle, retries, result paging.  |
| `configuration.json` | Connector configuration (see below).                              |
| `pyproject.toml` / `uv.lock` | Local dev dependencies, managed with uv.                  |
| `requirements.txt`   | Fivetran deploy deps — empty (only the pre-installed `requests`). |

## Configuration

All Fivetran configuration values are strings.

| Key                    | Required | Default                      | Description                                                                 |
| ---------------------- | -------- | ---------------------------- | --------------------------------------------------------------------------- |
| `api_key`              | Yes      | —                            | User-backed Unify API key, sent as `X-Api-Key`.                             |
| `base_url`             | No       | `https://api.unifygtm.com`   | API base URL.                                                               |
| `resources`            | No       | all tables                   | Comma-separated subset of tables to sync.                                   |
| `page_size`            | No       | `1000`                       | JSON result page size (max `2000`).                                         |
| `poll_timeout_seconds` | No       | `900`                        | Max time to wait for a single job to finish.                                |
| `object_selects`       | No       | sensible defaults            | JSON object overriding the attributes selected per object table.            |

Generate an API key in
[Settings → Developers](https://app.unifygtm.com/dashboard/settings/integrations/api-keys).

### Customizing object attributes

Object-record jobs require a `select`. Defaults live in `DEFAULT_OBJECT_SELECTS`
in `connector.py`. To override without editing code, set `object_selects` to a
JSON string, e.g.:

```json
{ "object_selects": "{\"company\": [\"name\", \"domain\", \"industry\"]}" }
```

## Run it locally

This project uses [uv](https://docs.astral.sh/uv/). Dependencies are declared in
`pyproject.toml` and pinned in `uv.lock`.

```bash
uv sync   # creates .venv and installs fivetran-connector-sdk + requests

# Put your real API key in a config file, then point the connector at it.
# Use a git-ignored copy to keep your key out of version control:
cp configuration.json configuration.local.json   # then edit in your api_key
uv run python connector.py --configuration configuration.local.json
```

`--configuration` defaults to `configuration.json`, so `uv run python
connector.py` works too if you've put the key there directly (but that file is
tracked in git — don't commit a real key).

(Plain `pip install fivetran-connector-sdk requests` into a venv works too.)

`connector.debug()` runs a full sync locally and writes the results to a DuckDB
warehouse file plus a `files/` directory so you can inspect the output. These
artifacts are git-ignored.

## Deploy to Fivetran

```bash
fivetran deploy --api-key <FIVETRAN_API_KEY> \
  --destination <DESTINATION> --connection <CONNECTION_NAME> \
  --configuration configuration.json
```

## Notes & limits

- **Rate limits:** job creation is ~100/day, so the connector creates one job
  per table per sync and relies on incremental cursors to keep each job focused.
  `429` responses are retried honoring `Retry-After`.
- **Job expiry:** jobs and results expire 24h after creation; a sync always
  downloads results immediately after the job finishes.
- **Result format:** JSON paging is used for a stable, page-based envelope. For
  very large pages the API also supports NDJSON (`Accept: application/x-ndjson`,
  up to `page_size=10000`) — a natural extension to `UnifyBulkClient`.
