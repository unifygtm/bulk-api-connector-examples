"""Thin client for the Unify Bulk API.

The Bulk API is asynchronous: you create a query job, poll the job until it
reaches a terminal status, then page through the materialized results. This
module wraps that lifecycle so the Fivetran connector in `connector.py` can stay
focused on schema/state concerns.

See: https://docs.unifygtm.com/developers/guides/request-data/bulk-api
"""

import time

import requests as rq

# For enabling logs in the connector. Falls back to print() when the connector
# SDK is not importable (e.g. running this module standalone in tests).
try:
    from fivetran_connector_sdk import Logging as log
except ImportError:  # pragma: no cover - convenience for standalone use

    class _Log:
        info = warning = severe = staticmethod(lambda msg: print(msg))

    log = _Log()


# Statuses that mean the job will not change again. Only FINISHED yields results.
TERMINAL_STATUSES = {"FINISHED", "FAILED", "CANCELED", "EXPIRED"}

# HTTP statuses we retry with backoff. 429 is rate limiting (honor Retry-After);
# 5xx are transient server errors.
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class BulkJobError(Exception):
    """Raised when a query job ends in a non-FINISHED terminal status."""

    def __init__(self, job_id, status, error_code=None):
        self.job_id = job_id
        self.status = status
        self.error_code = error_code
        super().__init__(
            f"Bulk job {job_id} ended as {status}"
            + (f" (error_code={error_code})" if error_code else "")
        )


class UnifyBulkClient:
    """Minimal client for the Unify Bulk API query-job lifecycle."""

    def __init__(
        self,
        api_key,
        base_url="https://api.unifygtm.com",
        poll_interval=2.0,
        poll_max_interval=30.0,
        poll_timeout=900.0,
        max_retries=5,
    ):
        if not api_key:
            raise ValueError("api_key is required for the Unify Bulk API")
        self.base_url = base_url.rstrip("/")
        self.poll_interval = poll_interval
        self.poll_max_interval = poll_max_interval
        self.poll_timeout = poll_timeout
        self.max_retries = max_retries

        self.session = rq.Session()
        self.session.headers.update(
            {
                "X-Api-Key": api_key,
                "Content-Type": "application/json",
            }
        )

    # -- low-level request with retry/backoff -------------------------------

    def _request(self, method, path, **kwargs):
        """Send a request, retrying rate-limit (429) and transient 5xx errors.

        `path` is a resource path like `/data/v1/events`; it is joined to the
        configured base URL.
        """
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            response = self.session.request(method, url, **kwargs)
            if response.status_code in RETRYABLE_STATUSES and attempt < self.max_retries:
                # Honor Retry-After when present, otherwise exponential backoff.
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else min(2**attempt, 30)
                log.warning(
                    f"{method} {path} -> {response.status_code}; retrying in {wait:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(wait)
                attempt += 1
                continue
            response.raise_for_status()
            return response

    # -- query-job lifecycle ------------------------------------------------

    def create_query_job(self, resource_base, body=None):
        """Create a query job for a resource. Returns the job metadata dict.

        `resource_base` is the resource path such as `/data/v1/objects/company`
        or `/sequences/v1/enrollments`.
        """
        response = self._request(
            "POST", f"{resource_base}/query-jobs", json=body or {}
        )
        return response.json()

    def get_job(self, resource_base, job_id):
        """Fetch current metadata for a single query job."""
        response = self._request("GET", f"{resource_base}/query-jobs/{job_id}")
        return response.json()

    def poll_job(self, resource_base, job_id):
        """Poll a job until it reaches a terminal status.

        Uses a steady interval that grows with exponential backoff (capped),
        as recommended by the Bulk API docs to avoid tight polling loops.
        Returns the final job metadata once FINISHED; raises BulkJobError for
        any other terminal status, and TimeoutError if polling exceeds the
        configured timeout.
        """
        deadline = time.monotonic() + self.poll_timeout
        interval = self.poll_interval
        while True:
            job = self.get_job(resource_base, job_id)
            status = job.get("status")
            if status == "FINISHED":
                return job
            if status in TERMINAL_STATUSES:
                raise BulkJobError(job_id, status, job.get("error_code"))

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Bulk job {job_id} did not finish within {self.poll_timeout}s "
                    f"(last status: {status})"
                )
            time.sleep(interval)
            interval = min(interval * 1.5, self.poll_max_interval)

    def cancel_job(self, resource_base, job_id):
        """Cancel an in-progress job. Returns the updated job metadata."""
        response = self._request(
            "POST", f"{resource_base}/query-jobs/{job_id}/cancel"
        )
        return response.json()

    # -- results ------------------------------------------------------------

    def iter_result_pages(self, resource_base, job_id, page_size=1000):
        """Yield result pages (lists of row dicts) for a FINISHED job.

        Uses the JSON results format, which returns a stable, page-based
        envelope: {"total", "page", "page_size", "data": [...]}. Pages are
        immutable for a finished job, so iteration is deterministic.
        """
        page = 1
        seen = 0
        while True:
            response = self._request(
                "GET",
                f"{resource_base}/query-jobs/{job_id}/results",
                params={"page": page, "page_size": page_size},
                headers={"Accept": "application/json"},
            )
            payload = response.json()
            rows = payload.get("data", [])
            if not rows:
                break

            yield rows

            seen += len(rows)
            total = payload.get("total", 0)
            if seen >= total or len(rows) < page_size:
                break
            page += 1
