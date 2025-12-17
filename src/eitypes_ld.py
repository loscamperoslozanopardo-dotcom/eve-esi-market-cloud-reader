#!/usr/bin/env python3
"""
EVE ESI -> BigQuery loader for Universe Types.

- Fetches all type_ids from /universe/types/ (paged).
- Retrieves details from /universe/types/{type_id}/ only as needed:
  - First run: all type_ids.
  - Next runs: incremental refresh for new type_ids, plus a rolling refresh window.
- Loads BigQuery tables (types, types_dt) with WRITE_TRUNCATE (no remnants).
- Persists only lightweight state (headers/meta) in state/eitypes_hdrs.json.

ESI notes:
- /universe/types/ is paged (page param) and provides X-Pages.
- Both /universe/types/ and /universe/types/{type_id}/ expire daily at 11:05 (ESI cache policy).
"""

from __future__ import annotations

import concurrent.futures as cf
import dataclasses
import hashlib
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery


# -------------------------
# Config
# -------------------------

@dataclasses.dataclass(frozen=True)
class Config:
    # BigQuery
    project_id: str
    project_number: str
    location: str
    dataset_id: str
    table_types: str
    table_types_dt: str

    # ESI
    esi_base: str
    esi_datasource: str
    esi_language: str
    user_agent: str

    # Runtime tuning
    esi_max_workers: int
    esi_max_rps: float

    # State
    state_file: str

    # Refresh strategy
    # Rolling refresh: each run refresh N random existing type_ids (helps catch silent changes without crawling all daily).
    rolling_refresh_n: int


def load_config() -> Config:
    def getenv(name: str, default: str) -> str:
        v = os.environ.get(name, default)
        return v.strip() if isinstance(v, str) else default

    max_workers = int(getenv("ESI_MAX_WORKERS", "20"))
    max_workers = max(1, min(max_workers, 40))

    max_rps = float(getenv("ESI_MAX_RPS", "15"))
    max_rps = max(0.5, min(max_rps, 50.0))

    rolling_n = int(getenv("ROLLING_REFRESH_N", "2000"))
    rolling_n = max(0, min(rolling_n, 20000))

    return Config(
        project_id=getenv("PROJECT_ID", ""),
        project_number=getenv("PROJECT_NUMBER", ""),
        location=getenv("LOCATION", "EU"),
        dataset_id=getenv("DATASET_ID", ""),
        table_types=getenv("TABLE_TYPES", "types"),
        table_types_dt=getenv("TABLE_TYPES_DT", "types_dt"),
        esi_base=getenv("ESI_BASE", "https://esi.evetech.net/latest").rstrip("/"),
        esi_datasource=getenv("ESI_DATASOURCE", "tranquility"),
        esi_language=getenv("ESI_LANGUAGE", "en"),
        user_agent=getenv("USER_AGENT", "eitypes loader"),
        esi_max_workers=max_workers,
        esi_max_rps=max_rps,
        state_file=getenv("STATE_FILE", "state/eitypes_hdrs.json"),
        rolling_refresh_n=rolling_n,
    )


# -------------------------
# Helpers
# -------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        logging.exception("Failed reading state file, starting fresh: %s", path)
        return {}


class GlobalRateLimiter:
    """
    Very simple global rate limiter (shared across threads).
    Enforces ~max_rps aggregate request rate.
    """

    def __init__(self, max_rps: float) -> None:
        self.min_interval = 1.0 / max_rps if max_rps > 0 else 0.0
        self._next_allowed = 0.0
        self._lock = None
        try:
            import threading
            self._lock = threading.Lock()
        except Exception:
            self._lock = None

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        if self._lock is None:
            # Fallback
            time.sleep(self.min_interval)
            return

        with self._lock:
            now = time.time()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
                now = time.time()
            self._next_allowed = now + self.min_interval


# -------------------------
# ESI HTTP layer
# -------------------------

def build_session(cfg: Config) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": cfg.user_agent,
            "Accept": "application/json",
            "Accept-Language": cfg.esi_language,
        }
    )
    return s


def esi_get_json(
    session: requests.Session,
    limiter: GlobalRateLimiter,
    url: str,
    params: Dict[str, Any],
    timeout: int = 30,
    max_attempts: int = 8,
) -> Tuple[Dict[str, Any], requests.Response]:
    """
    GET JSON with backoff.
    Handles:
      - 429 retry-after
      - 420 error limited (ESI)
      - 5xx transient
    """
    backoff = 1.0
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            limiter.wait()
            resp = session.get(url, params=params, timeout=timeout)

            # Basic ESI error limiting headers (best-effort; may not always be present)
            remain = resp.headers.get("X-ESI-Error-Limit-Remain") or resp.headers.get("x-esi-error-limit-remain")
            reset = resp.headers.get("X-ESI-Error-Limit-Reset") or resp.headers.get("x-esi-error-limit-reset")
            if remain is not None and reset is not None:
                try:
                    r = int(remain)
                    rs = int(reset)
                    if r <= 10:
                        logging.warning("ESI error-limit remain low (%s). Sleeping %ss", r, rs)
                        time.sleep(max(0, rs) + 1)
                except Exception:
                    pass

            if resp.status_code == 200:
                return resp.json(), resp

            if resp.status_code == 304:
                return {}, resp  # caller interprets 304

            if resp.status_code in (420, 429):
                ra = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
                sleep_s = None
                if ra:
                    try:
                        sleep_s = int(float(ra))
                    except Exception:
                        sleep_s = None
                if sleep_s is None and reset:
                    try:
                        sleep_s = int(reset)
                    except Exception:
                        sleep_s = None
                sleep_s = sleep_s if sleep_s is not None else int(backoff)
                logging.warning("Rate limited (%s). Sleeping %ss (attempt %s/%s)", resp.status_code, sleep_s, attempt, max_attempts)
                time.sleep(max(1, sleep_s))
                backoff = min(backoff * 1.8, 60.0)
                continue

            if 500 <= resp.status_code <= 599:
                logging.warning("ESI %s on %s. attempt %s/%s", resp.status_code, url, attempt, max_attempts)
                time.sleep(backoff + random.random())
                backoff = min(backoff * 1.8, 60.0)
                continue

            # non-retryable
            resp.raise_for_status()

        except Exception as e:
            last_exc = e
            logging.warning("Request error (attempt %s/%s): %s", attempt, max_attempts, e)
            time.sleep(backoff + random.random())
            backoff = min(backoff * 1.8, 60.0)

    raise RuntimeError(f"Failed after {max_attempts} attempts: {url}") from last_exc


def fetch_all_type_ids(cfg: Config, session: requests.Session, limiter: GlobalRateLimiter) -> Tuple[List[int], Dict[str, str]]:
    """
    Fetches all pages of /universe/types/ and returns:
      - full list of type_ids
      - "best" headers snapshot (etag/last-modified/expires from page1)
    """
    url = f"{cfg.esi_base}/universe/types/"
    params_base = {"datasource": cfg.esi_datasource}

    # Page 1 to discover X-Pages
    j1, r1 = esi_get_json(session, limiter, url, params={**params_base, "page": 1})
    if r1.status_code != 200 or not isinstance(j1, list):
        raise RuntimeError(f"Unexpected response for page 1: status={r1.status_code}")

    x_pages = r1.headers.get("X-Pages") or r1.headers.get("x-pages") or "1"
    try:
        pages = int(x_pages)
    except Exception:
        pages = 1

    headers_snapshot = {
        "etag": r1.headers.get("ETag", ""),
        "last_modified": r1.headers.get("Last-Modified", ""),
        "expires": r1.headers.get("Expires", ""),
        "x_pages": str(pages),
    }

    type_ids: List[int] = []
    type_ids.extend(int(x) for x in j1)

    if pages <= 1:
        return type_ids, headers_snapshot

    # Fetch remaining pages concurrently (payload is small: ints)
    def get_page(p: int) -> List[int]:
        jp, rp = esi_get_json(session, limiter, url, params={**params_base, "page": p})
        if rp.status_code != 200 or not isinstance(jp, list):
            raise RuntimeError(f"Unexpected response for page {p}: status={rp.status_code}")
        return [int(x) for x in jp]

    with cf.ThreadPoolExecutor(max_workers=min(cfg.esi_max_workers, max(2, pages))) as ex:
        futures = [ex.submit(get_page, p) for p in range(2, pages + 1)]
        for fut in cf.as_completed(futures):
            type_ids.extend(fut.result())

    return type_ids, headers_snapshot


def fetch_type_detail(
    cfg: Config,
    session: requests.Session,
    limiter: GlobalRateLimiter,
    type_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Fetch /universe/types/{type_id}/ and return a normalized dict or None (if not published).
    """
    url = f"{cfg.esi_base}/universe/types/{type_id}/"
    params = {"datasource": cfg.esi_datasource, "language": cfg.esi_language}
    j, r = esi_get_json(session, limiter, url, params=params)

    if r.status_code != 200:
        # 404 can happen for invalid IDs; ignore safely
        if r.status_code == 404:
            return None
        raise RuntimeError(f"Unexpected status for type_id={type_id}: {r.status_code}")

    # published filter
    published = bool(j.get("published", False))
    if not published:
        return None

    # Fields required by your table definition
    name = j.get("name")
    group_id = j.get("group_id")
    market_group_id = j.get("market_group_id", None)
    packaged_volume = j.get("packaged_volume", None)
    volume = j.get("volume", None)

    if name is None or group_id is None:
        # required in schema for most types; skip if malformed
        return None

    row = {
        "type_id": int(type_id),
        "type_name": str(name),
        "group_id": int(group_id),
        "market_group_id": int(market_group_id) if market_group_id is not None else None,
        # ESI schema defines these as float numbers; keep float in BQ to avoid losing decimals
        "packaged_volume": float(packaged_volume) if packaged_volume is not None else None,
        "volume": float(volume) if volume is not None else None,
    }
    return row


# -------------------------
# BigQuery
# -------------------------

def bq_client(cfg: Config) -> bigquery.Client:
    return bigquery.Client(project=cfg.project_id)


def ensure_dataset(cfg: Config, client: bigquery.Client) -> None:
    ds_id = f"{cfg.project_id}.{cfg.dataset_id}"
    ds = bigquery.Dataset(ds_id)
    ds.location = cfg.location
    try:
        client.create_dataset(ds, exists_ok=True)
    except Exception:
        # exists_ok should cover most cases; keep a safe fallback
        pass


def ensure_tables(cfg: Config, client: bigquery.Client) -> None:
    ensure_dataset(cfg, client)

    types_table_id = f"{cfg.project_id}.{cfg.dataset_id}.{cfg.table_types}"
    types_dt_table_id = f"{cfg.project_id}.{cfg.dataset_id}.{cfg.table_types_dt}"

    # types schema (FLOAT64 for volumes because ESI returns float)
    types_schema = [
        bigquery.SchemaField("type_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("type_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("group_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("market_group_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("packaged_volume", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("volume", "FLOAT64", mode="NULLABLE"),
    ]

    types_dt_schema = [
        bigquery.SchemaField("expires_utc", "TIMESTAMP", mode="REQUIRED"),
    ]

    for table_id, schema in [(types_table_id, types_schema), (types_dt_table_id, types_dt_schema)]:
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.create_table(table)
            logging.info("Created table %s", table_id)
        except Conflict:
            pass


def table_exists(client: bigquery.Client, table_id: str) -> bool:
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def read_existing_types(cfg: Config, client: bigquery.Client) -> Dict[int, Dict[str, Any]]:
    """
    Reads existing types table into dict[type_id] = rowdict.
    If table doesn't exist, returns empty dict.
    """
    table_id = f"{cfg.project_id}.{cfg.dataset_id}.{cfg.table_types}"
    if not table_exists(client, table_id):
        return {}

    sql = f"""
    SELECT
      type_id, type_name, group_id, market_group_id, packaged_volume, volume
    FROM `{table_id}`
    """
    rows = client.query(sql).result()
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        tid = int(r["type_id"])
        out[tid] = {
            "type_id": tid,
            "type_name": r["type_name"],
            "group_id": int(r["group_id"]),
            "market_group_id": int(r["market_group_id"]) if r["market_group_id"] is not None else None,
            "packaged_volume": float(r["packaged_volume"]) if r["packaged_volume"] is not None else None,
            "volume": float(r["volume"]) if r["volume"] is not None else None,
        }
    return out


def write_types(cfg: Config, client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    table_id = f"{cfg.project_id}.{cfg.dataset_id}.{cfg.table_types}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=[
            bigquery.SchemaField("type_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("type_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("group_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("market_group_id", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("packaged_volume", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume", "FLOAT64", mode="NULLABLE"),
        ],
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    logging.info("Loaded %s rows into %s", len(rows), table_id)


def write_types_dt(cfg: Config, client: bigquery.Client, expires_utc: datetime) -> None:
    table_id = f"{cfg.project_id}.{cfg.dataset_id}.{cfg.table_types_dt}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=[bigquery.SchemaField("expires_utc", "TIMESTAMP", mode="REQUIRED")],
    )
    payload = [{"expires_utc": expires_utc.isoformat()}]
    job = client.load_table_from_json(payload, table_id, job_config=job_config)
    job.result()
    logging.info("Loaded types_dt into %s (expires_utc=%s)", table_id, expires_utc.isoformat())


# -------------------------
# Main orchestration
# -------------------------

def parse_http_expires(expires_str: str) -> Optional[datetime]:
    # ESI uses RFC7231 HTTP-date; parsing robustly without extra deps
    if not expires_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(expires_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def pick_rolling_refresh(existing_ids: List[int], n: int) -> List[int]:
    if n <= 0 or not existing_ids:
        return []
    if n >= len(existing_ids):
        return list(existing_ids)
    # stable-ish randomness
    return random.sample(existing_ids, n)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    cfg = load_config()
    if not cfg.project_id or not cfg.dataset_id:
        logging.error("Missing PROJECT_ID or DATASET_ID env.")
        return 2

    state = read_json(cfg.state_file)
    state.setdefault("meta", {})
    state.setdefault("esi", {})
    state.setdefault("bq", {})

    session = build_session(cfg)
    limiter = GlobalRateLimiter(cfg.esi_max_rps)

    # 1) Fetch all type_ids (paged)
    logging.info("Fetching /universe/types/ (paged)...")
    type_ids, list_hdrs = fetch_all_type_ids(cfg, session, limiter)
    type_ids = sorted(set(type_ids))
    logging.info("Got %s type_ids total (unique). X-Pages=%s", len(type_ids), list_hdrs.get("x_pages"))

    list_hash = sha256_hex(",".join(map(str, type_ids)).encode("utf-8"))
    prev_hash = state.get("esi", {}).get("types_list_hash")

    # 2) BigQuery setup
    client = bq_client(cfg)
    ensure_tables(cfg, client)

    # 3) Load existing table snapshot (for incremental merge)
    existing = read_existing_types(cfg, client)
    existing_ids = sorted(existing.keys())
    logging.info("Existing BQ types rows: %s", len(existing_ids))

    # Decide if we can skip:
    # - If list hasn't changed AND we already have a non-empty table, we skip heavy work.
    if prev_hash == list_hash and len(existing_ids) > 0:
        logging.info("types_list_hash unchanged and BQ already populated. Skipping update.")
        # still keep headers updated in state (lightweight)
        state["esi"]["types_list_headers"] = list_hdrs
        state["esi"]["types_list_hash"] = list_hash
        state["meta"]["last_checked_utc"] = iso_z(utcnow())
        atomic_write_json(cfg.state_file, state)
        return 0

    # 4) Determine which ids to fetch:
    # Always fetch missing ids + a rolling refresh window to gradually re-validate data.
    missing = sorted(set(type_ids) - set(existing_ids))
    rolling = pick_rolling_refresh(existing_ids, cfg.rolling_refresh_n)
    to_fetch = sorted(set(missing).union(rolling))

    logging.info("Will fetch details for %s type_ids (missing=%s, rolling=%s)",
                 len(to_fetch), len(missing), len(rolling))

    # 5) Fetch details concurrently
    fetched_rows: Dict[int, Dict[str, Any]] = {}

    def worker(tid: int) -> Tuple[int, Optional[Dict[str, Any]]]:
        row = fetch_type_detail(cfg, session, limiter, tid)
        return tid, row

    # Use bounded concurrency
    with cf.ThreadPoolExecutor(max_workers=cfg.esi_max_workers) as ex:
        futures = [ex.submit(worker, tid) for tid in to_fetch]
        done = 0
        for fut in cf.as_completed(futures):
            tid, row = fut.result()
            done += 1
            if done % 500 == 0:
                logging.info("Progress: %s/%s fetched...", done, len(futures))
            if row is not None:
                fetched_rows[tid] = row

    logging.info("Fetched published rows: %s", len(fetched_rows))

    # 6) Build final dataset:
    # - Start from existing published rows.
    # - Remove rows whose type_id no longer exists in list.
    # - Apply fetched updates/additions.
    current_id_set = set(type_ids)

    merged: Dict[int, Dict[str, Any]] = {}
    for tid, row in existing.items():
        if tid in current_id_set:
            merged[tid] = row

    for tid, row in fetched_rows.items():
        if tid in current_id_set:
            merged[tid] = row

    final_rows = [merged[tid] for tid in sorted(merged.keys())]
    logging.info("Final published rows to load: %s", len(final_rows))

    # 7) Write to BigQuery (types + types_dt)
    expires_dt = parse_http_expires(list_hdrs.get("expires", "")) or utcnow()
    write_types(cfg, client, final_rows)
    write_types_dt(cfg, client, expires_dt)

    # 8) Persist state ONLY after successful BQ load
    state["esi"]["types_list_headers"] = list_hdrs
    state["esi"]["types_list_hash"] = list_hash
    state["meta"]["last_success_request_utc"] = iso_z(utcnow())
    state["meta"]["last_success_expires_utc"] = iso_z(expires_dt)
    state["bq"]["last_loaded_rows"] = len(final_rows)

    atomic_write_json(cfg.state_file, state)

    # Print success timestamp (requested)
    logging.info("SUCCESS request_utc=%s expires_utc=%s rows=%s",
                 state["meta"]["last_success_request_utc"],
                 state["meta"]["last_success_expires_utc"],
                 state["bq"]["last_loaded_rows"])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logging.exception("Fatal error")
        sys.exit(1)
