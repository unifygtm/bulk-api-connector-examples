"""Fivetran Connector SDK connector for the Unify Bulk API.

This connector syncs data out of Unify (https://unifygtm.com) using its
asynchronous Bulk API. Each synced table maps to one Bulk API resource:

    table                       resource base path
    -----                       ------------------
    company                     /data/v1/objects/company
    person                      /data/v1/objects/person
    opportunity                 /data/v1/objects/opportunity
    event                       /data/v1/events
    sequence_enrollment         /sequences/v1/enrollments
    sequence_enrollment_step    /sequences/v1/enrollment-steps
    task                        /tasks/v1/tasks

For each table the connector creates a query job, polls it to completion, then
pages through the results and upserts each row. Syncs are incremental: the
connector stores the high-water mark of each table's cursor field (`updated_at`
for object records, sequences, and tasks; `timestamp` for the immutable event
stream, the only datetime the events resource can filter on) in Fivetran state
and uses it to scope the next job's filter.

See the Bulk API guide:
https://docs.unifygtm.com/developers/guides/request-data/bulk-api
"""

import json

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from unify_bulk import BulkJobError, UnifyBulkClient

# Cursor value used on the first sync (and after a full re-sync, when state is
# empty). Effectively "from the beginning of time".
DEFAULT_CURSOR = "1970-01-01T00:00:00Z"

# Default attribute selects for object-record resources. The Bulk API requires
# a `select` for object jobs; we list the scalar standard attributes so each
# table comes out flat. Composite values (`address`, and the company `revenue`
# currency, which returns `{code, value}`) and reference attributes are left
# out because they come back as nested objects; `flatten_row` would store them
# as JSON text. Opportunity `amount` has no currency code and stays a plain
# number. Add or remove attribute API names here to customize the columns, or
# override per deployment via the `object_selects` configuration. Selecting an
# attribute your workspace does not have fails job creation with a 400, so
# check `GET /data/v1/objects/{object}/attributes` before adding one.
DEFAULT_OBJECT_SELECTS = {
    "company": [
        "name",
        "domain",
        "description",
        "industry",
        "employee_count",
        "founded",
        "status",
        "lead_source",
        "linkedin_url",
        "corporate_phone",
        "do_not_contact",
        "last_activity_at",
    ],
    "person": [
        "email",
        "first_name",
        "last_name",
        "title",
        "status",
        "lead_source",
        "linkedin_url",
        "corporate_phone",
        "mobile_phone",
        "work_phone",
        "do_not_call",
        "do_not_email",
        "email_opt_out",
        "eu_resident",
        "last_activity_at",
    ],
    "opportunity": [
        "name",
        "uniqueness_key",
        "amount",
        "stage",
        "opportunity_type",
        "lead_source",
        "original_created_at",
    ],
}

# Resource catalog. `kind` controls how the query-job body and cursor are built:
#   - "object":   POST body is a `query` with select/sort_by/metadata; results
#                 are sorted ascending by the cursor field, so we can advance the
#                 checkpoint page by page.
#   - "events" /  POST body is an optional `filter`; results are not guaranteed
#     "sequence" /  to be ordered by the cursor field, so we checkpoint once at
#     "task"        the end of the job using the max cursor value observed.
RESOURCES = {
    "company": {
        "base": "/data/v1/objects/company",
        "kind": "object",
        "cursor_field": "updated_at",
    },
    "person": {
        "base": "/data/v1/objects/person",
        "kind": "object",
        "cursor_field": "updated_at",
    },
    "opportunity": {
        "base": "/data/v1/objects/opportunity",
        "kind": "object",
        "cursor_field": "updated_at",
    },
    "event": {
        "base": "/data/v1/events",
        "kind": "events",
        # The events resource filters and orders on `timestamp`; it returns no
        # `created_at`.
        "cursor_field": "timestamp",
    },
    "sequence_enrollment": {
        "base": "/sequences/v1/enrollments",
        "kind": "sequence",
        "cursor_field": "updated_at",
    },
    "sequence_enrollment_step": {
        "base": "/sequences/v1/enrollment-steps",
        "kind": "sequence",
        "cursor_field": "updated_at",
    },
    "task": {
        "base": "/tasks/v1/tasks",
        "kind": "task",
        "cursor_field": "updated_at",
    },
}


def schema(configuration: dict):
    """Declare the tables this connector delivers.

    We declare the primary key and known timestamp columns so they land with
    the correct types; any other attributes returned by the Bulk API are
    inferred by Fivetran from the upserted data.
    See https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    """
    return [
        {
            "table": "company",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "person",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "opportunity",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "original_created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "event",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "timestamp": "UTC_DATETIME",
            },
        },
        {
            "table": "sequence_enrollment",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "started_at": "UTC_DATETIME",
                "ended_at": "UTC_DATETIME",
            },
        },
        {
            # Enrollment steps carry no `created_at`; `updated_at` is the cursor.
            "table": "sequence_enrollment_step",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "updated_at": "UTC_DATETIME",
                "started_at": "UTC_DATETIME",
                "ended_at": "UTC_DATETIME",
            },
        },
        {
            "table": "task",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "note_content": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
                "due_at": "UTC_DATETIME",
                "ended_at": "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Run an incremental sync of every selected resource.

    See https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    """
    log.info("Starting Unify Bulk API sync")

    client = UnifyBulkClient(
        api_key=configuration["api_key"],
        base_url=configuration.get("base_url", "https://api.unifygtm.com"),
        poll_timeout=float(configuration.get("poll_timeout_seconds", 900)),
    )

    object_selects = _resolve_object_selects(configuration)
    page_size = int(configuration.get("page_size", 1000))
    tables = _selected_tables(configuration)

    for table in tables:
        spec = RESOURCES[table]
        selects = object_selects.get(table) if spec["kind"] == "object" else None
        sync_resource(client, state, table, spec, selects, page_size)

    log.info("Unify Bulk API sync complete")


def sync_resource(client, state, table, spec, selects, page_size):
    """Create, poll, and drain a single resource's query job into `table`."""
    cursor_field = spec["cursor_field"]
    cursor = state.get(table, {}).get("cursor", DEFAULT_CURSOR)

    body = build_job_body(spec, selects, cursor)
    log.info(f"[{table}] creating query job (cursor {cursor_field} >= {cursor})")

    created = client.create_query_job(spec["base"], body)
    job_id = created["job_id"]

    try:
        job = client.poll_job(spec["base"], job_id)
    except BulkJobError as err:
        # A failed/canceled/expired job for one resource should not abort the
        # whole sync; log it and move on. State for this table is untouched, so
        # the next sync retries from the same cursor.
        log.severe(f"[{table}] {err}")
        return

    total = job.get("total_rows", 0)
    log.info(f"[{table}] job {job_id} finished with {total} rows")

    # For object resources, results are sorted ascending by the cursor field, so
    # we can advance the checkpoint per page. For unsorted resources we hold the
    # cursor until the job is fully drained, then checkpoint the observed max.
    advance_per_page = spec["kind"] == "object"
    max_cursor = cursor

    for rows in client.iter_result_pages(spec["base"], job_id, page_size):
        for row in rows:
            op.upsert(table=table, data=flatten_row(row))
            value = row.get(cursor_field)
            if value and value > max_cursor:
                max_cursor = value

        if advance_per_page:
            state[table] = {"cursor": max_cursor}
            op.checkpoint(state)

    # Final checkpoint: advances unsorted resources, and is a harmless no-op
    # restate for sorted ones.
    state[table] = {"cursor": max_cursor}
    op.checkpoint(state)


def build_job_body(spec, selects, cursor):
    """Build the POST body for a query job, scoped to records after `cursor`."""
    kind = spec["kind"]
    if kind == "object":
        return {
            "query": {
                "select": {name: True for name in selects},
                "sort_by": {"field": "updated_at", "direction": "ASCENDING"},
                "metadata": {"updated_at": {"gte": cursor}},
            }
        }
    # Events, sequences, and tasks take an optional `filter`. Scope by the
    # cursor field so each job only returns new or changed records.
    return {"filter": {spec["cursor_field"]: {"gte": cursor}}}


def flatten_row(row):
    """Flatten one result row into destination columns.

    Object-record rows nest the attributes you selected under `attributes`
    (alongside `object`, `id`, `created_at`, `updated_at`); those are lifted to
    top-level columns so the table matches the `select`. Attributes with no
    value are omitted from the row entirely, so a column only appears once some
    record has it. Base record fields win over a like-named attribute.

    Anything still nested after that (reference expansions, currency values
    like company `revenue`, event `properties`, `company`, and `person`, and
    the sequence resources' `sequence`, `person`, `mailbox`, `enrollment`,
    `enrolled_by_play`, `email_message`, and `reply_email_message` objects) is
    stored as JSON text. Scalars pass through unchanged.
    """
    attributes = row.get("attributes")
    merged = dict(attributes) if isinstance(attributes, dict) else {}
    merged.update({key: value for key, value in row.items() if key != "attributes"})

    flat = {}
    for key, value in merged.items():
        if isinstance(value, (dict, list)):
            flat[key] = json.dumps(value)
        else:
            flat[key] = value
    return flat


def _selected_tables(configuration):
    """Return the ordered list of tables to sync, honoring `resources` config."""
    configured = configuration.get("resources")
    if not configured:
        return list(RESOURCES)
    requested = [name.strip() for name in configured.split(",") if name.strip()]
    unknown = [name for name in requested if name not in RESOURCES]
    if unknown:
        raise ValueError(
            f"Unknown resources in configuration: {unknown}. "
            f"Valid options: {list(RESOURCES)}"
        )
    return requested


def _resolve_object_selects(configuration):
    """Merge any `object_selects` override (JSON string) over the defaults."""
    selects = {table: list(cols) for table, cols in DEFAULT_OBJECT_SELECTS.items()}
    override = configuration.get("object_selects")
    if override:
        selects.update(json.loads(override))
    return selects


# Create the connector object that Fivetran drives via the schema/update functions.
connector = Connector(update=update, schema=schema)

# Local debugging entry point. Fivetran does not call this in production; run
# `python connector.py --configuration configuration.json` to test against a
# real API key. The SDK's `debug()` does not auto-load configuration.json, so we
# read it here and pass it through explicitly.
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run a local Unify Bulk API sync.")
    parser.add_argument(
        "--configuration",
        default="configuration.json",
        help="Path to a configuration JSON file (default: configuration.json).",
    )
    args = parser.parse_args()

    with open(args.configuration) as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
