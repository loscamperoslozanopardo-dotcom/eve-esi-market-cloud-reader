#!/usr/bin/env python3
"""EVE ESI -> BigQuery loader: Universe systems.

Loads a daily snapshot of solar systems into:
  - eve-market-sandbox.eve_market.systems
  - eve-market-sandbox.eve_market.systems_dt

Key behaviors:
- Uses conditional requests (If-None-Match / If-Modified-Since) against
  /v1/universe/systems/ to avoid unnecessary work.
- Respects ESI caching: if cached Expires is still in the future, exit early.
- Tracks ESI error limiting via X-ESI-Error-Limit-* headers.
- Rewrites the destination tables on each successful load.

State file (persisted via GitHub Actions cache): state/eus_hdrs.json
"""

from __future__ import annotations

import io
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# ----------------------------
# Configuration (env-overridable)
# ----------------------------

PROJECT_ID = os.getenv("BQ_PROJECT_ID", "eve-market-sandbox")
DATASET_ID = os.getenv("BQ_DATASET_ID", "eve_market")
BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

TABLE_SYSTEMS = "systems"
TABLE_SYSTEMS_DT = "systems_dt"

STATE_PATH = os.getenv("EUS_STATE_PATH", os.path.join("state", "eus_hdrs.json"))

ESI_BASE_URL = os.getenv("ESI_BASE_URL", "https://esi.evetech.net")
ESI_DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")

# ESI endpoints (versioned routes)
SYSTEMS_LIST_PATH = "/v1/universe/systems/"
SYSTEM_DETAIL_PATH = "/v4/universe/systems/{system_id}/"

# Concurrency / politeness knobs
MAX_WORKERS = int(os.getenv("EUS_WORKERS", "20"))
GLOBAL_QPS = float(os.getenv("EUS_QPS", "20"))  # global request rate cap across threads
ERROR_LIMIT_PAUSE_THRESHOLD = int(os.getenv("EUS_ERROR_LIMIT_THRESHOLD", "5"))

# Requests
HTTP_TIMEOUT_S = float(os.getenv("EUS_HTTP_TIMEOUT_S", "30"))

# Identify your app to CCP (best practice)
DEFAULT_UA = (
    "eve-market-sandbox (EUS loader; contact: "
    "https://github.com/loscamperoslozanopardo-dotcom/eve-esi-market-cloud-reader)"
)
ESI_USER_AGENT = os.getenv("ESI_USER_AGENT", DEFAULT_UA)

# ----------------------------
# Utilities
# ----------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def httpdate_to_utc(ts: str) -> datetime:
    """Parse RFC7231 HTTP-date into aware UTC datetime."""
    dt = parsedate_to_datetime(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_rfc3339(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    # BigQuery accepts RFC3339. Keep a "Z" suffix.
    return dt.isoformat().replace("+00:00", "Z")


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
        return json.loads(raw) if raw else {}


def atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def build_url(path: str) -> str:
    return f"{ESI_BASE_URL}{path}"


# ----------------------------
# Request throttling & error-limit handling
# ----------------------------


@dataclass
class ErrorLimitState:
    remain: Optional[int] = None
    reset_seconds: Optional[int] = None


class RequestController:
    """A small global controller for:
    - global QPS limiting
    - pausing when ESI error-limit is close/exceeded

    It is intentionally simple and defensive.
    """

    def __init__(self, qps: float) -> None:
        self._lock = threading.Lock()
        self._min_interval = 1.0 / max(qps, 0.1)
        self._next_request_at = 0.0
        self._pause_until = 0.0

    def wait_turn(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                pause = max(self._pause_until - now, 0.0)
                wait = max(self._next_request_at - now, 0.0)
                sleep_for = max(pause, wait)
                if sleep_for <= 0:
                    self._next_request_at = now + self._min_interval
                    return
            time.sleep(min(sleep_for, 5.0))

    def pause(self, seconds: float) -> None:
        if seconds <= 0:
            return
        with self._lock:
            self._pause_until = max(self._pause_until, time.monotonic() + seconds)

    def update_from_headers(self, headers: Dict[str, str]) -> ErrorLimitState:
        remain = headers.get("X-ESI-Error-Limit-Remain") or headers.get("x-esi-error-limit-remain")
        reset = headers.get("X-ESI-Error-Limit-Reset") or headers.get("x-esi-error-limit-reset")

        st = ErrorLimitState()
        try:
            if remain is not None:
                st.remain = int(remain)
            if reset is not None:
                st.reset_seconds = int(reset)
        except ValueError:
            return st

        # Proactive pause if we are about to be error-limited.
        if st.remain is not None and st.reset_seconds is not None:
            if st.remain <= ERROR_LIMIT_PAUSE_THRESHOLD:
                # add a small buffer to avoid racing
                self.pause(float(st.reset_seconds) + 1.0)

        return st


_thread_local = threading.local()


def session() -> requests.Session:
    """Thread-local requests.Session."""
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": ESI_USER_AGENT,
            }
        )
        _thread_local.session = sess
    return sess


def request_with_retries(
    controller: RequestController,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    allow_304: bool = False,
    max_attempts: int = 5,
) -> requests.Response:
    """Perform an HTTP request with:
    - global QPS limiting
    - backoff for transient errors
    - pause on error-limit headers / 420 / 429
    """
    backoff = 1.0
    attempt = 0

    while True:
        attempt += 1
        controller.wait_turn()

        try:
            resp = session().request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=HTTP_TIMEOUT_S,
            )
        except requests.RequestException as e:
            if attempt >= max_attempts:
                raise RuntimeError(f"Request failed after {attempt} attempts: {url}") from e
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 30.0)
            continue

        controller.update_from_headers(resp.headers)

        if resp.status_code == 304 and allow_304:
            return resp

        if resp.status_code in (420, 429):
            # error limited / too many requests
            retry_after = resp.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    wait_s = float(retry_after)
                except ValueError:
                    wait_s = 60.0
            else:
                st = controller.update_from_headers(resp.headers)
                wait_s = float(st.reset_seconds or 60)
            controller.pause(wait_s + 1.0)
            if attempt >= max_attempts:
                raise RuntimeError(f"HTTP {resp.status_code} persisted after {attempt} attempts for {url}")
            continue

        if resp.status_code >= 500:
            if attempt >= max_attempts:
                raise RuntimeError(f"HTTP {resp.status_code} after {attempt} attempts for {url}: {resp.text[:200]}")
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 30.0)
            continue

        # other non-2xx -> fail fast
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code} for {url}: {resp.text[:500]}")

        return resp


# ----------------------------
# BigQuery helpers
# ----------------------------


def bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def ensure_table(client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]) -> None:
    try:
        client.get_table(table_id)
        return
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)


def load_ndjson_to_table(
    client: bigquery.Client,
    table_id: str,
    rows: Iterable[Dict[str, Any]],
    schema: List[bigquery.SchemaField],
) -> int:
    buf = io.StringIO()
    count = 0
    for row in rows:
        buf.write(json.dumps(row, separators=(",", ":")))
        buf.write("\n")
        count += 1

    data = buf.getvalue().encode("utf-8")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_file(
        io.BytesIO(data),
        table_id,
        job_config=job_config,
        location=BQ_LOCATION,
    )
    job.result()
    return count


# ----------------------------
# Main
# ----------------------------


def main() -> int:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = load_state(STATE_PATH)

    cached_expires = state.get("expires")
    if isinstance(cached_expires, str):
        try:
            expires_dt = httpdate_to_utc(cached_expires)
            # Respect caching: don't update before Expires.
            if utc_now() < expires_dt:
                print(f"[eus] Cache still valid until {to_rfc3339(expires_dt)}; skipping run.")
                return 0
        except Exception:
            # If parsing fails, just proceed.
            pass

    controller = RequestController(GLOBAL_QPS)

    # 1) Fetch the list of system IDs with conditional headers.
    list_headers: Dict[str, str] = {}
    if isinstance(state.get("etag"), str) and state["etag"]:
        list_headers["If-None-Match"] = state["etag"]
    if isinstance(state.get("last_modified"), str) and state["last_modified"]:
        list_headers["If-Modified-Since"] = state["last_modified"]

    list_url = build_url(SYSTEMS_LIST_PATH)
    resp = request_with_retries(
        controller,
        "GET",
        list_url,
        headers=list_headers,
        params={"datasource": ESI_DATASOURCE},
        allow_304=True,
    )

    # Extract cache headers (present in both 200 and 304 per ESI spec).
    new_expires = resp.headers.get("Expires")
    new_etag = resp.headers.get("ETag")
    new_last_modified = resp.headers.get("Last-Modified")

    if resp.status_code == 304:
        # Persist header state; no BigQuery work needed.
        next_state = dict(state)
        if new_expires:
            next_state["expires"] = new_expires
        if new_etag:
            next_state["etag"] = new_etag
        if new_last_modified:
            next_state["last_modified"] = new_last_modified

        # Consider the request successful (even if payload unchanged).
        next_state["last_success_request_utc"] = to_rfc3339(utc_now())
        atomic_write_json(STATE_PATH, next_state)
        print("[eus] ESI list: 304 Not Modified; headers updated; nothing to load.")
        return 0

    systems_ids: List[int] = resp.json()
    if not isinstance(systems_ids, list) or not systems_ids:
        raise RuntimeError("ESI returned an unexpected payload for systems list")

    # 2) Fetch detail for each system (we only extract required fields).
    def fetch_one(system_id: int) -> Dict[str, Any]:
        url = build_url(SYSTEM_DETAIL_PATH.format(system_id=system_id))
        r = request_with_retries(
            controller,
            "GET",
            url,
            params={"datasource": ESI_DATASOURCE},
            allow_304=False,
        )
        payload = r.json()
        pos = payload.get("position") or {}
        return {
            "system_id": int(payload["system_id"]),
            "system_name": str(payload["name"]),
            "constellation_id": int(payload["constellation_id"]),
            "position_x": float(pos.get("x")),
            "position_y": float(pos.get("y")),
            "position_z": float(pos.get("z")),
            "security_status": float(payload.get("security_status")),
        }

    rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_one, sid): sid for sid in systems_ids}
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                rows.append(fut.result())
            except Exception as e:
                errors.append(f"system_id={sid}: {e}")

    if errors:
        # Fail the run so the workflow retries, without updating persisted headers.
        preview = "\n".join(errors[:20])
        raise RuntimeError(f"Failed to fetch {len(errors)} system details. First errors:\n{preview}")

    # 3) Load into BigQuery (rewrite tables).
    client = bq_client()
    table_systems_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_SYSTEMS}"
    table_dt_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_SYSTEMS_DT}"

    systems_schema = [
        bigquery.SchemaField("system_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("system_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("constellation_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("position_x", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("position_y", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("position_z", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("security_status", "FLOAT", mode="REQUIRED"),
    ]
    dt_schema = [
        bigquery.SchemaField("expires_utc", "TIMESTAMP", mode="REQUIRED"),
    ]

    ensure_table(client, table_systems_id, systems_schema)
    ensure_table(client, table_dt_id, dt_schema)

    loaded = load_ndjson_to_table(client, table_systems_id, rows, systems_schema)

    if not new_expires:
        raise RuntimeError("Missing Expires header on ESI systems list response")
    expires_dt = httpdate_to_utc(new_expires)

    load_ndjson_to_table(
        client,
        table_dt_id,
        [{"expires_utc": to_rfc3339(expires_dt)}],
        dt_schema,
    )

    # 4) Persist state only after successful BigQuery loads.
    next_state = dict(state)
    if new_expires:
        next_state["expires"] = new_expires
    if new_etag:
        next_state["etag"] = new_etag
    if new_last_modified:
        next_state["last_modified"] = new_last_modified

    # "Hora de la petición" (only if end-to-end success):
    next_state["last_success_request_utc"] = to_rfc3339(utc_now())
    next_state["last_success_rows"] = int(loaded)

    atomic_write_json(STATE_PATH, next_state)

    print(
        "[eus] Loaded systems into BigQuery: "
        f"rows={loaded}, expires_utc={to_rfc3339(expires_dt)}, "
        f"request_utc={next_state['last_success_request_utc']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[eus] ERROR: {exc}", file=sys.stderr)
        raise
