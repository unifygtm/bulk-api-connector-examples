# Unify Bulk API — Airbyte custom connector

A low-code [Airbyte](https://airbyte.com) source connector that syncs data out
of [Unify](https://unifygtm.com) using its asynchronous
[Bulk API](https://docs.unifygtm.com/developers/guides/request-data/bulk-api).

The Bulk API is built for exporting large datasets without long-lived HTTP
requests. Airbyte's [`AsyncRetriever`](https://docs.airbyte.com/connector-development/connector-builder-ui/async-streams) models that lifecycle directly.

For each resource it:

1. **Creates a query job** (`POST {base}/{resource}/query-jobs`), scoped to
   records changed since the last sync (`creation_requester`).
2. **Polls the job** (`GET {base}/{resource}/query-jobs/{job_id}`) until its
   status maps to `completed` (`polling_requester` + `status_mapping`).
3. **Pages through results** (`GET {base}/{resource}/query-jobs/{job_id}/results`)
   and emits each row (`download_requester` + `download_paginator`).
4. **Tracks a cursor** (`DatetimeBasedCursor`) so the next sync only requests new
   or changed data.

## Streams

| Stream                     | Bulk resource                    | Cursor field |
| -------------------------- | -------------------------------- | ------------ |
| `company`                  | `/data/v1/objects/company`       | `updated_at` |
| `person`                   | `/data/v1/objects/person`        | `updated_at` |
| `opportunity`              | `/data/v1/objects/opportunity`   | `updated_at` |
| `event`                    | `/data/v1/events`                | `timestamp`  |
| `sequence_enrollment`      | `/sequences/v1/enrollments`      | `updated_at` |
| `sequence_enrollment_step` | `/sequences/v1/enrollment-steps` | `updated_at` |
| `task`                     | `/tasks/v1/tasks`                | `updated_at` |
| `object_definitions`       | `/data/v1/objects`               | —            |

(`user` is also a standard object but is left out — it's an internal
reference target for `record_owner`, not a primary export stream.)

`object_definitions` is the odd one out: a small, synchronous full-refresh
stream that lists the objects available to your API key (a plain `GET`, not a
Bulk job). It exists primarily to back the connection `check` (see below), but
it's a registered stream so it also shows up in the catalog.

## Project layout

| File                                        | Purpose                                                                               |
| ------------------------------------------- | ------------------------------------------------------------------------------------- |
| `manifest.yaml`                             | The whole connector: shared definitions, the streams, `check`, `spec`.                |
| `metadata.yaml`                             | Connector id, image repo/tag, and the declarative-manifest base image.                |
| `config.example.json`                       | Template config; copy to `secrets/config.json` and add your API key.                  |
| `build_config.py`                           | Merges `manifest.yaml` into your config for the `source-declarative-manifest` runner. |
| `integration_tests/configured_catalog.json` | Committed read catalog (all streams; Bulk streams incremental, `object_definitions` full-refresh); passed to `read`. |

Record schemas are declared inline in `manifest.yaml` (`InlineSchemaLoader`):
known columns are typed and `additionalProperties: true` lets any other
attribute the Bulk API returns flow through.

## Configuration

| Key                    | Required | Default                    | Description                                               |
| ---------------------- | -------- | -------------------------- | --------------------------------------------------------- |
| `api_key`              | Yes      | —                          | User-backed Unify API key, sent as `X-Api-Key`.           |
| `base_url`             | No       | `https://api.unifygtm.com` | API base URL.                                             |
| `start_date`           | No       | `1970-01-01T00:00:00Z`     | Only sync records changed at or after this UTC timestamp. |
| `page_size`            | No       | `1000`                     | JSON result page size (max `2000`).                       |
| `poll_timeout_minutes` | No       | `60`                       | Max minutes to wait for a single job to finish.           |
| `num_workers`          | No       | `2`                        | Streams to sync in parallel (1–7). See concurrency note.  |

Generate an API key in
[Settings → Developers](https://app.unifygtm.com/dashboard/settings/integrations/api-keys).

## Build, test, and run it locally

This connector uses Airbyte's low-code tooling. Install the CDK with
[uv](https://docs.astral.sh/uv/) — this provides the `source-declarative-manifest`
runner used below:

```bash
uv tool install --upgrade 'airbyte-cdk[dev]'
```

Because this is a manifest-only connector, it runs through the
`source-declarative-manifest` runner, which takes the standard Airbyte verbs
(`spec`/`check`/`discover`/`read`). The runner has no `--manifest-path` flag — it
reads the manifest from the config under the `__injected_declarative_manifest`
key. Put your API key in a git-ignored config, then merge the manifest into it
with the included helper (re-run the helper whenever you edit `manifest.yaml` or
the config):

```bash
mkdir -p secrets
cp config.example.json secrets/config.json   # then edit in your api_key
uv run build_config.py                        # writes secrets/merged_config.json
```

Now validate and run the connector's operations:

```bash
# Inspect the configuration spec the connector exposes (no config needed).
source-declarative-manifest spec

# Check the connection — issues a single synchronous GET /data/v1/objects (the
# `object_definitions` stream), so it validates your API key without creating a
# Bulk job. A bad key surfaces here as 401.
source-declarative-manifest check --config secrets/merged_config.json

# Discover the streams and their schemas.
source-declarative-manifest discover --config secrets/merged_config.json
```

`read` needs a configured catalog. A ready-to-use one (all streams — Bulk
streams incremental, `object_definitions` full-refresh) is committed at
`integration_tests/configured_catalog.json`, so you can read straight away
(incremental state is printed as it advances):

```bash
source-declarative-manifest read --config secrets/merged_config.json \
  --catalog integration_tests/configured_catalog.json
```

Tip: set a recent `start_date` in `secrets/config.json` (e.g.
`"2024-06-01T00:00:00Z"`) so the first job is small instead of a full backfill,
and trim the catalog to a single stream to test one at a time.

> **CDK version note:** the manifest is pinned to CDK `7.23.1` (see `version:`
> in `manifest.yaml` and the base image in `metadata.yaml`), and the commands
> above run fine on the latest `airbyte-cdk`. If you need to match the
> production runtime exactly, run the verbs through a pinned, Python 3.12
> interpreter, e.g.
> `uvx --python 3.12 --from 'airbyte-cdk[dev]==7.23.1' source-declarative-manifest check --config secrets/merged_config.json`.

The fastest way to iterate is the [Connector Builder](https://docs.airbyte.com/connector-development/connector-builder-ui/overview):
import `manifest.yaml`, fill in your API key, and use the live test panel to step
through job creation → polling → download per stream.

## Deploy to Airbyte

Build the connector image and register it with your Airbyte instance:

```bash
# Build the manifest-only image (uses metadata.yaml's base image).
airbyte-ci connectors --name source-unify-bulk build
```

Then add it as a custom connector in Airbyte
([Settings → Sources → + New connector](https://docs.airbyte.com/integrations/custom-connectors)),
pointing at the image from `metadata.yaml` (`dockerRepository` : `dockerImageTag`),
or upload `manifest.yaml` directly via the Connector Builder.

## Notes & limits

- **Async lifecycle:** one job is created per stream per sync; the
  `AsyncRetriever` polls until `status` is `FINISHED` before downloading.
  Adjust `poll_timeout_minutes` for large jobs.
- **Rate limits:** job creation is limited (~100/day). Incremental cursors keep
  each job focused. `429`/`5xx` responses are retried (up to `max_retries: 5`);
  the `base_requester`'s `error_handler` uses a `WaitTimeFromHeader` backoff
  strategy so 429 retries wait for the duration in the API's `Retry-After`
  header rather than a default exponential backoff.
- **Concurrency:** the manifest's `concurrency_level` (`ConcurrencyLevel`) syncs
  up to `num_workers` streams in parallel (default `2`, ceiling `7` — one per
  Bulk stream), and `max_concurrent_async_job_count: 1` caps in-flight Bulk jobs per
  stream (each sync issues exactly one). Each parallel stream creates another
  job, so keep `num_workers` modest to stay under the job-creation rate limit.
- **Job expiry:** jobs and results expire 24h after creation; each sync
  downloads results immediately after the job finishes.
- **Result format:** the connector uses JSON paging (stable, page-based
  envelope). For very large pages the API also supports NDJSON
  (`Accept: application/x-ndjson`, up to `page_size=10000`) — switch the
  `download_requester`'s `Accept` header and decoder to adopt it.
