import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_THIS_DIR = os.path.dirname(__file__)
_LIB_DIR = os.path.join(_THIS_DIR, "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from emsp_esi_http import (
    fetch_markets_prices,
    now_utc,
    parse_http_date,
)
from emsp_bq_ld import (
    ensure_dataset,
    load_prices_to_bigquery_from_list,
    write_prices_dt_table,
)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip() in ("1", "true", "True", "yes", "YES")


def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json_atomic(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    # ---- env (BQ)
    project_id = os.getenv("PROJECT_ID", "eve-market-sandbox")
    location = os.getenv("LOCATION", "EU")
    dataset_id = os.getenv("DATASET_ID", "eve_market")
    table_prices = os.getenv("TABLE_PRICES", "markets_prices")
    table_prices_dt = os.getenv("TABLE_PRICES_DT", "markets_prices_dt")

    # ---- env (ESI)
    datasource = os.getenv("ESI_DATASOURCE", "tranquility")
    user_agent = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (github actions)")

    # ---- env (policy)
    state_file = os.getenv("STATE_FILE", os.path.join("state", "emsp_hdrs.json"))
    force_verify_after_expires = _env_bool("FORCE_VERIFY_AFTER_EXPIRES", True)

    # ---- env (robust)
    timeout_s = _env_int("TIMEOUT_S", 60)
    max_retries = _env_int("MAX_RETRIES", 5)
    error_limit_threshold = _env_int("ERROR_LIMIT_THRESHOLD", 20)

    state: Dict[str, Any] = _load_json(state_file, {}) if state_file else {}
    if not isinstance(state, dict):
        state = {}

    # Política Expires: si NO expiró y la política lo indica, no hacemos request.
    prev_expires = state.get("expires")
    prev_expires_dt = parse_http_date(prev_expires) if isinstance(prev_expires, str) else None
    now = now_utc()

    if force_verify_after_expires and prev_expires_dt and now < prev_expires_dt:
        print(
            json.dumps(
                {
                    "note": "skip_not_expired",
                    "now_utc": _iso_z(now),
                    "expires_utc": _iso_z(prev_expires_dt),
                },
                ensure_ascii=False,
            )
        )
        return 0

    # 1) Fetch ESI (usa If-None-Match / If-Modified-Since)
    result = fetch_markets_prices(
        prev_state=state,
        datasource=datasource,
        user_agent=user_agent,
        timeout_s=timeout_s,
        max_retries=max_retries,
        error_limit_threshold=error_limit_threshold,
    )

    status = result["status"]
    headers = result["headers"]
    data = result["data"]

    # Éxito ESI: 200 o 304 (ETag best practices)
    if status not in (200, 304):
        raise RuntimeError(f"Unexpected ESI status: {status}")

    # 2) Validación Expires (requisito para markets_prices_dt)
    expires = headers.get("Expires") or headers.get("expires")
    expires_dt = parse_http_date(expires)
    if not expires_dt:
        raise RuntimeError("Missing/invalid Expires header on successful response; cannot write markets_prices_dt")

    request_utc = now_utc()

    # 3) BigQuery: dataset + dt table (siempre en éxito ESI)
    ensure_dataset(project_id=project_id, dataset_id=dataset_id, location=location)
    write_prices_dt_table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_prices_dt,
        expires_dt=expires_dt,
        location=location,
    )

    # 4) BigQuery: prices table SOLO si hay 200 con datos
    loaded_rows: Optional[int] = None
    if status == 200:
        if not isinstance(data, list):
            raise RuntimeError("ESI payload is not a list")
        # Normaliza registros (y evita basura accidental)
        cleaned = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if "type_id" not in item:
                continue
            cleaned.append(
                {
                    "type_id": item.get("type_id"),
                    "average_price": item.get("average_price"),
                    "adjusted_price": item.get("adjusted_price"),
                }
            )
        loaded_rows = load_prices_to_bigquery_from_list(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_prices,
            rows=cleaned,
            location=location,
        )

    # 5) Persist state SOLO tras éxito (ESI ok + BQ ok)
    # Guardamos headers relevantes y “hora de petición” solo si exitosa.
    new_state = dict(state)
    new_state["etag"] = headers.get("ETag") or headers.get("etag") or new_state.get("etag")
    new_state["last_modified"] = headers.get("Last-Modified") or headers.get("last-modified") or new_state.get("last_modified")
    new_state["expires"] = expires
    new_state["last_success_request_utc"] = _iso_z(request_utc)
    new_state["last_success_http_status"] = status
    new_state["last_success_expires_utc"] = _iso_z(expires_dt)
    if loaded_rows is not None:
        new_state["last_success_loaded_rows"] = loaded_rows

    if state_file:
        _save_json_atomic(state_file, new_state)

    # salida “visible”
    print(
        json.dumps(
            {
                "status": status,
                "request_utc": _iso_z(request_utc),
                "expires_utc": _iso_z(expires_dt),
                "loaded_rows": loaded_rows,
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
        # el workflow maneja el retry a los 5 min
        print(f"::error::{type(e).__name__}: {e}")
        raise
