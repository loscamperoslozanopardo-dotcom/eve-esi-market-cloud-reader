import json
import os
import time
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery


# -----------------------------
# ESI endpoints
# -----------------------------
ESI_BASE_DEFAULT = "https://esi.evetech.net/latest"
REGIONS_LIST_PATH = "/universe/regions/"
REGION_INFO_PATH_TMPL = "/universe/regions/{region_id}/"


# -----------------------------
# Small utils
# -----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_http_date(value: Optional[str]) -> Optional[datetime]:
    """Parse RFC7231-ish HTTP date to UTC datetime."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip() in ("1", "true", "True", "yes", "YES")


def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_atomic(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# -----------------------------
# BigQuery helpers
# -----------------------------
def ensure_dataset(project_id: str, dataset_id: str, location: str) -> None:
    client = bigquery.Client(project=project_id)
    ds = bigquery.Dataset(f"{project_id}.{dataset_id}")
    ds.location = location
    client.create_dataset(ds, exists_ok=True)


def load_regions_to_bigquery_from_list(
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: List[Dict[str, Any]],
    location: str,
) -> int:
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("region_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("region_name", "STRING", mode="REQUIRED"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
    )

    payload = (
        "\n".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) for r in rows)
        + "\n"
    ).encode("utf-8")

    job = client.load_table_from_file(
        BytesIO(payload), table_ref, job_config=job_config, location=location
    )
    job.result()
    return len(rows)


def write_regions_dt_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    expires_dt: datetime,
    location: str,
) -> None:
    client = bigquery.Client(project=project_id)

    if expires_dt.tzinfo is None:
        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    expires_dt = expires_dt.astimezone(timezone.utc)

    sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
    SELECT @expires_utc AS expires_utc
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expires_utc", "TIMESTAMP", expires_dt)
        ]
    )

    job = client.query(sql, job_config=job_config, location=location)
    job.result()


# -----------------------------
# ESI HTTP (with basic ESI error-limit protection)
# -----------------------------
def _read_int(headers: Dict[str, str], name: str) -> Optional[int]:
    v = headers.get(name) or headers.get(name.lower())
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None


_thread_local = threading.local()


def _session(user_agent: str) -> requests.Session:
    # requests.Session is not guaranteed thread-safe; use thread-local sessions.
    sess = getattr(_thread_local, "sess", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip"})
        _thread_local.sess = sess
    return sess


def _esi_get(
    url: str,
    params: Dict[str, Any],
    user_agent: str,
    timeout_s: int,
    max_retries: int,
    error_limit_threshold: int,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, str], Optional[Any]]:
    """GET JSON from ESI with minimal retry/backoff.

    Returns: (status_code, response_headers, json_data_or_None)
    """
    sess = _session(user_agent)
    req_headers = headers or {}

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = sess.get(url, params=params, headers=req_headers, timeout=timeout_s)
        except requests.RequestException as e:
            if attempt == max_retries:
                raise RuntimeError(
                    f"ESI request failed after {max_retries} attempts: {e}"
                ) from e
            time.sleep(backoff)
            backoff = min(10.0, backoff * 2)
            continue

        hdrs = {k: v for k, v in r.headers.items()}

        # 429: obey Retry-After
        if r.status_code == 429:
            ra = hdrs.get("Retry-After") or hdrs.get("retry-after") or "5"
            try:
                wait_s = float(ra)
            except Exception:
                wait_s = 5.0
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(wait_s + 0.5)
            continue

        # ESI error-limit protection
        remain = _read_int(hdrs, "X-ESI-Error-Limit-Remain")
        reset = _read_int(hdrs, "X-ESI-Error-Limit-Reset")
        if remain is not None and remain <= error_limit_threshold:
            time.sleep(float((reset or 10) + 1))

        # 5xx retry
        if 500 <= r.status_code <= 599:
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(backoff)
            backoff = min(10.0, backoff * 2)
            continue

        # Other 4xx (except 304)
        if 400 <= r.status_code <= 499 and r.status_code != 304:
            r.raise_for_status()

        if r.status_code == 304:
            return 304, hdrs, None

        # Any 2xx with JSON body
        return r.status_code, hdrs, r.json()

    raise RuntimeError("unreachable: retries loop exhausted")


def fetch_regions_list(
    prev_state: Dict[str, Any],
    esi_base: str,
    datasource: str,
    user_agent: str,
    timeout_s: int,
    max_retries: int,
    error_limit_threshold: int,
) -> Dict[str, Any]:
    """Fetch /universe/regions/ with ETag/Last-Modified conditional headers."""
    url = f"{esi_base}{REGIONS_LIST_PATH}"

    cond_headers: Dict[str, str] = {}
    prev_etag = prev_state.get("etag")
    if isinstance(prev_etag, str) and prev_etag:
        cond_headers["If-None-Match"] = prev_etag

    prev_lm = prev_state.get("last_modified")
    if isinstance(prev_lm, str) and prev_lm:
        cond_headers["If-Modified-Since"] = prev_lm

    status, hdrs, data = _esi_get(
        url=url,
        params={"datasource": datasource},
        headers=cond_headers,
        user_agent=user_agent,
        timeout_s=timeout_s,
        max_retries=max_retries,
        error_limit_threshold=error_limit_threshold,
    )

    return {"status": status, "headers": hdrs, "data": data}


def fetch_region_info(
    region_id: int,
    esi_base: str,
    datasource: str,
    language: str,
    user_agent: str,
    timeout_s: int,
    max_retries: int,
    error_limit_threshold: int,
) -> Dict[str, Any]:
    """Fetch /universe/regions/{region_id}/ (body always needed; do NOT use 304)."""
    url = f"{esi_base}{REGION_INFO_PATH_TMPL.format(region_id=region_id)}"

    status, _hdrs, data = _esi_get(
        url=url,
        params={"datasource": datasource, "language": language},
        headers=None,
        user_agent=user_agent,
        timeout_s=timeout_s,
        max_retries=max_retries,
        error_limit_threshold=error_limit_threshold,
    )

    if status != 200 or not isinstance(data, dict):
        raise RuntimeError(
            f"Unexpected region info response for {region_id}: status={status}"
        )

    name = data.get("name")
    if not isinstance(name, str) or not name:
        raise RuntimeError(f"Missing/invalid 'name' for region {region_id}")

    return {"region_id": int(region_id), "region_name": name}


# -----------------------------
# Main
# -----------------------------
@dataclass(frozen=True)
class RunConfig:
    # BigQuery
    project_id: str
    dataset_id: str
    location: str
    table_regions: str
    table_regions_dt: str

    # ESI
    esi_base: str
    datasource: str
    language: str
    user_agent: str

    # Policy
    state_file: str
    force_verify_after_expires: bool

    # Robustness
    timeout_s: int
    max_retries: int
    error_limit_threshold: int
    workers: int


def _load_state(path: str) -> Dict[str, Any]:
    state = load_json(path, {}) if path else {}
    return state if isinstance(state, dict) else {}


def main() -> int:
    # ---- env (BQ)
    project_id = os.getenv("PROJECT_ID", "eve-market-sandbox")
    location = os.getenv("LOCATION", "EU")
    dataset_id = os.getenv("DATASET_ID", "eve_market")
    table_regions = os.getenv("TABLE_REGIONS", "regions")
    table_regions_dt = os.getenv("TABLE_REGIONS_DT", "regions_dt")

    # ---- env (ESI)
    esi_base = os.getenv("ESI_BASE", ESI_BASE_DEFAULT)
    datasource = os.getenv("ESI_DATASOURCE", "tranquility")
    language = os.getenv("ESI_LANGUAGE", "en")
    user_agent = os.getenv(
        "USER_AGENT",
        "eve-esi-market-cloud-reader (github actions)",
    )

    # ---- env (policy)
    state_file = os.getenv("STATE_FILE", os.path.join("state", "eur_hdrs.json"))
    force_verify_after_expires = env_bool("FORCE_VERIFY_AFTER_EXPIRES", True)

    # ---- env (robust)
    timeout_s = env_int("TIMEOUT_S", 60)
    max_retries = env_int("MAX_RETRIES", 5)
    error_limit_threshold = env_int("ERROR_LIMIT_THRESHOLD", 20)
    workers = env_int("WORKERS", 10)
    workers = max(1, min(workers, 20))

    cfg = RunConfig(
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        table_regions=table_regions,
        table_regions_dt=table_regions_dt,
        esi_base=esi_base,
        datasource=datasource,
        language=language,
        user_agent=user_agent,
        state_file=state_file,
        force_verify_after_expires=force_verify_after_expires,
        timeout_s=timeout_s,
        max_retries=max_retries,
        error_limit_threshold=error_limit_threshold,
        workers=workers,
    )

    state = _load_state(cfg.state_file)

    # Expires policy: if we have a valid Expires in state and it's still in the future,
    # and policy is enabled, do nothing.
    prev_expires = state.get("expires")
    prev_expires_dt = parse_http_date(prev_expires) if isinstance(prev_expires, str) else None
    now = now_utc()

    if cfg.force_verify_after_expires and prev_expires_dt and now < prev_expires_dt:
        print(
            json.dumps(
                {
                    "note": "skip_not_expired",
                    "now_utc": iso_z(now),
                    "expires_utc": iso_z(prev_expires_dt),
                },
                ensure_ascii=False,
            )
        )
        return 0

    # 1) Fetch region IDs list (conditional with ETag / Last-Modified)
    res = fetch_regions_list(
        prev_state=state,
        esi_base=cfg.esi_base,
        datasource=cfg.datasource,
        user_agent=cfg.user_agent,
        timeout_s=cfg.timeout_s,
        max_retries=cfg.max_retries,
        error_limit_threshold=cfg.error_limit_threshold,
    )

    status = int(res["status"])
    headers = res["headers"]
    data = res["data"]

    if status not in (200, 304):
        raise RuntimeError(f"Unexpected ESI status for regions list: {status}")

    expires = headers.get("Expires") or headers.get("expires")
    expires_dt = parse_http_date(expires)
    if not expires_dt:
        raise RuntimeError(
            "Missing/invalid Expires header on successful response; cannot write regions_dt"
        )

    request_utc = now_utc()

    # 2) BigQuery: ensure dataset, write dt table (always for successful ESI list)
    ensure_dataset(project_id=cfg.project_id, dataset_id=cfg.dataset_id, location=cfg.location)
    write_regions_dt_table(
        project_id=cfg.project_id,
        dataset_id=cfg.dataset_id,
        table_id=cfg.table_regions_dt,
        expires_dt=expires_dt,
        location=cfg.location,
    )

    loaded_rows: Optional[int] = None

    # 3) If modified (200), resolve region names and rewrite regions table
    if status == 200:
        if not isinstance(data, list):
            raise RuntimeError("ESI regions list payload is not a list")

        # Ensure we have integers
        region_ids: List[int] = []
        for x in data:
            try:
                region_ids.append(int(x))
            except Exception:
                continue

        if not region_ids:
            raise RuntimeError("ESI regions list returned no valid region IDs")

        # Fetch region info in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

        max_workers = min(cfg.workers, len(region_ids))
        rows: List[Dict[str, Any]] = []
        errors: List[str] = []

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [
                ex.submit(
                    fetch_region_info,
                    region_id,
                    cfg.esi_base,
                    cfg.datasource,
                    cfg.language,
                    cfg.user_agent,
                    cfg.timeout_s,
                    cfg.max_retries,
                    cfg.error_limit_threshold,
                )
                for region_id in region_ids
            ]

            for fut in as_completed(futs):
                try:
                    rows.append(fut.result())
                except Exception as e:
                    errors.append(str(e))

        if errors:
            raise RuntimeError(
                f"Failed to fetch {len(errors)} region(s). Example: {errors[0]}"
            )

        # Stable ordering
        rows.sort(key=lambda r: int(r["region_id"]))

        loaded_rows = load_regions_to_bigquery_from_list(
            project_id=cfg.project_id,
            dataset_id=cfg.dataset_id,
            table_id=cfg.table_regions,
            rows=rows,
            location=cfg.location,
        )

    # 4) Persist state only after end-to-end success (ESI ok + BQ ok)
    new_state = dict(state)
    new_state["etag"] = headers.get("ETag") or headers.get("etag") or new_state.get("etag")
    new_state["last_modified"] = headers.get("Last-Modified") or headers.get("last-modified") or new_state.get("last_modified")
    new_state["expires"] = expires
    new_state["last_success_request_utc"] = iso_z(request_utc)
    new_state["last_success_http_status"] = status
    new_state["last_success_expires_utc"] = iso_z(expires_dt)
    if loaded_rows is not None:
        new_state["last_success_loaded_rows"] = loaded_rows

    if cfg.state_file:
        save_json_atomic(cfg.state_file, new_state)

    print(
        json.dumps(
            {
                "status": status,
                "request_utc": iso_z(request_utc),
                "expires_utc": iso_z(expires_dt),
                "loaded_rows": loaded_rows,
                "workers": min(cfg.workers, (len(data) if isinstance(data, list) else 0)) if status == 200 else 0,
                "note": "bq_loaded" if status == 200 else "not_modified_304",
            },
            ensure_ascii=False,
        )
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        # The workflow handles the 10-minute retries.
        print(f"::error::{type(e).__name__}: {e}")
        raise
