import json
import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, Any, Optional, List, Tuple
import requests

ESI_BASE = "https://esi.evetech.net/latest"
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")
USER_AGENT = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (worker)")
STATE_PATH = os.getenv("STATE_PATH", "state/market_state.json")
PLAN_PATH = os.getenv("PLAN_PATH", "plan/plan.json")
OUT_DIR = os.getenv("OUT_DIR", "out")

BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
BATCH_WORKERS = int(os.getenv("BATCH_WORKERS", "3"))
MAX_REGIONS_THIS_RUN = int(os.getenv("MAX_REGIONS_THIS_RUN", "9999"))  # tope “soft” si quieres

# Ritmo conservador
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.10"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.25"))

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

def sleep_polite():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def load_json(path: str, default: Any) -> Any:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def parse_http_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def read_error_limit(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    remain = headers.get("X-ESI-Error-Limit-Remain") or headers.get("x-esi-error-limit-remain")
    reset = headers.get("X-ESI-Error-Limit-Reset") or headers.get("x-esi-error-limit-reset")
    try:
        return (int(remain) if remain is not None else None,
                int(reset) if reset is not None else None)
    except ValueError:
        return (None, None)

def error_limit_guard(headers: Dict[str, str], threshold: int = 20):
    remain, reset = read_error_limit(headers)
    if remain is not None and remain <= threshold:
        wait_s = (reset or 10) + 1
        print(f"[WARN] Error-limit bajo (remain={remain}). Sleep {wait_s}s")
        time.sleep(wait_s)

def get_page(region_id: int, page: int, no_cache: bool = False) -> Tuple[Any, Dict[str, str]]:
    url = f"{ESI_BASE}/markets/{region_id}/orders/"
    params = {"datasource": DATASOURCE, "order_type": "all", "page": str(page)}
    headers = {}
    # no_cache=True => no usamos If-None-Match (queremos 200 para snapshot)
    r = session.get(url, params=params, headers=headers, timeout=60)
    error_limit_guard(dict(r.headers))
    r.raise_for_status()
    return r.json(), dict(r.headers)

def assign_batches(eligible: List[Dict[str, Any]], workers: int) -> List[List[Dict[str, Any]]]:
    """
    Bin packing simple basado en tu pages_acum, pero sin max_expiration:
    asigna regiones (completas) a workers para balancear peso (X-Pages).
    """
    bins = [[] for _ in range(workers)]
    loads = [0 for _ in range(workers)]

    # Ordena por peso descendente (Longest Processing Time)
    eligible_sorted = sorted(eligible, key=lambda r: r["weight_pages"], reverse=True)

    for rec in eligible_sorted:
        # asigna al bin con menor carga
        i = min(range(workers), key=lambda k: loads[k])
        bins[i].append(rec)
        loads[i] += rec["weight_pages"]

    return bins

def main():
    plan = load_json(PLAN_PATH, None)
    if not plan:
        raise RuntimeError("No existe plan.json (job scan no corrió o no se descargó artifact).")

    state = load_json(STATE_PATH, {"regions": {}})

    eligible = plan.get("eligible", [])
    # limit soft por si quieres
    eligible = eligible[:MAX_REGIONS_THIS_RUN]

    batches = assign_batches(eligible, BATCH_WORKERS)
    my_batch = batches[BATCH_INDEX] if BATCH_INDEX < len(batches) else []

    os.makedirs(OUT_DIR, exist_ok=True)
    run_manifest = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "batch_index": BATCH_INDEX,
        "batch_workers": BATCH_WORKERS,
        "regions_in_batch": [r["region_id"] for r in my_batch],
        "snapshots": [],
        "errors": [],
    }

    for rec in my_batch:
        rid = rec["region_id"]
        entry = state["regions"].setdefault(str(rid), {})
        try:
            # Page 1 (si estamos aquí, es porque is_due o bootstrap)
            page1, h1 = get_page(rid, 1, no_cache=True)
            lm_ref = (h1.get("Last-Modified") or h1.get("last-modified") or "")
            x_pages = int((h1.get("X-Pages") or h1.get("x-pages") or rec.get("weight_pages") or 1))

            region_dir = os.path.join(OUT_DIR, "orders", f"region_{rid}")
            os.makedirs(region_dir, exist_ok=True)

            with open(os.path.join(region_dir, "page_1.json"), "w", encoding="utf-8") as f:
                json.dump(page1, f)

            total = len(page1)

            for page in range(2, x_pages + 1):
                data, hp = get_page(rid, page, no_cache=True)
                lm = (hp.get("Last-Modified") or hp.get("last-modified") or "")
                if lm_ref and lm and (lm != lm_ref):
                    raise RuntimeError(f"Snapshot inconsistente: Last-Modified cambió (ref={lm_ref}, got={lm})")

                total += len(data)
                with open(os.path.join(region_dir, f"page_{page}.json"), "w", encoding="utf-8") as f:
                    json.dump(data, f)
                sleep_polite()

            entry["last_full_snapshot_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            entry["last_snapshot_last_modified"] = lm_ref
            entry["x_pages_last"] = x_pages
            entry["fail_streak"] = 0

            run_manifest["snapshots"].append({"region_id": rid, "pages": x_pages, "orders": total, "last_modified": lm_ref})
        except Exception as e:
            entry["fail_streak"] = int(entry.get("fail_streak") or 0) + 1
            entry["last_error"] = str(e)
            run_manifest["errors"].append({"region_id": rid, "error": str(e)})

    save_json(STATE_PATH, state)
    save_json(os.path.join(OUT_DIR, f"manifest_batch_{BATCH_INDEX}.json"), run_manifest)

    print(json.dumps({
        "batch_index": BATCH_INDEX,
        "regions": len(my_batch),
        "snapshots": len(run_manifest["snapshots"]),
        "errors": len(run_manifest["errors"]),
    }, indent=2))

if __name__ == "__main__":
    main()
