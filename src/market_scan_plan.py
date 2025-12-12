import json
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, Any, Optional, List
import requests

ESI_BASE = "https://esi.evetech.net/latest"
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")
USER_AGENT = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (scan)")
STATE_PATH = os.getenv("STATE_PATH", "state/market_state.json")
PLAN_PATH = os.getenv("PLAN_PATH", "plan/plan.json")

# Bootstrap: 1 = permitir snapshot inicial aunque no haya expirado (solo si no existe snapshot previo)
BOOTSTRAP = os.getenv("BOOTSTRAP", "1") == "1"

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

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

def load_json(path: str, default: Any) -> Any:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def get_regions() -> List[int]:
    url = f"{ESI_BASE}/universe/regions/"
    r = session.get(url, params={"datasource": DATASOURCE}, timeout=45)
    r.raise_for_status()
    return sorted(r.json())

def head_page1(region_id: int, etag: Optional[str]) -> Dict[str, Any]:
    url = f"{ESI_BASE}/markets/{region_id}/orders/"
    params = {"datasource": DATASOURCE, "order_type": "all", "page": "1"}
    headers = {}
    if etag:
        headers["If-None-Match"] = etag  # ETag best practice :contentReference[oaicite:5]{index=5}
    r = session.get(url, params=params, headers=headers, timeout=45)

    info = {
        "status": r.status_code,
        "etag": r.headers.get("ETag") or r.headers.get("etag"),
        "expires": (r.headers.get("Expires") or r.headers.get("expires")),
        "last_modified": (r.headers.get("Last-Modified") or r.headers.get("last-modified")),
        "x_pages": (r.headers.get("X-Pages") or r.headers.get("x-pages")),
    }

    if r.status_code == 304:
        return info

    r.raise_for_status()
    return info

def main():
    state = load_json(STATE_PATH, {"regions": {}})
    regions = get_regions()
    now = now_utc()

    plan_regions = []
    eligible = []

    for rid in regions:
        entry = state["regions"].setdefault(str(rid), {})
        info = head_page1(rid, entry.get("etag_page1"))

        # actualiza estado
        if info.get("etag"):
            entry["etag_page1"] = info["etag"]
        if info.get("expires"):
            entry["expires"] = info["expires"]
        if info.get("last_modified"):
            entry["last_modified"] = info["last_modified"]
        if info.get("x_pages"):
            try:
                entry["x_pages_last"] = int(info["x_pages"])
            except Exception:
                pass

        exp_dt = parse_http_date(entry.get("expires"))
        has_snapshot = bool(entry.get("last_full_snapshot_at"))

        # elegible si:
        # - exp_dt existe y now >= exp_dt
        # - o bootstrap activo y no hay snapshot previo
        is_due = (exp_dt is not None and now >= exp_dt)
        is_bootstrap = (BOOTSTRAP and (not has_snapshot))

        # Peso para planificar (X-Pages)
        weight = int(entry.get("x_pages_last") or 1)

        rec = {
            "region_id": rid,
            "weight_pages": weight,
            "expires": entry.get("expires"),
            "last_modified": entry.get("last_modified"),
            "etag_page1": entry.get("etag_page1"),
            "is_due": is_due,
            "is_bootstrap": is_bootstrap,
            "has_snapshot": has_snapshot,
        }
        plan_regions.append(rec)

        if is_due or is_bootstrap:
            eligible.append(rec)

    # Orden de prioridad:
    # 1) las más atrasadas (Expires más antiguo) primero
    # 2) más pesadas primero
    def priority(rec: Dict[str, Any]):
        exp = parse_http_date(rec.get("expires"))
        exp_ts = exp.timestamp() if exp else 0.0
        return (exp_ts, -rec["weight_pages"])

    eligible_sorted = sorted(eligible, key=priority)

    plan = {
        "timestamp_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions_total": len(regions),
        "eligible_count": len(eligible_sorted),
        "eligible": eligible_sorted,
        "all_regions": plan_regions,
        "notes": {
            "respect_expires": "No refrescar antes de Expires (ESI advierte contra circumvention).",
            "etag": "Usar If-None-Match/ETag; 304 => seguir usando datos previos y nuevos headers.",
        },
    }

    save_json(STATE_PATH, state)
    save_json(PLAN_PATH, plan)

    print(json.dumps({
        "timestamp_utc": plan["timestamp_utc"],
        "regions_total": plan["regions_total"],
        "eligible_count": plan["eligible_count"],
        "bootstrap": BOOTSTRAP,
    }, indent=2))

if __name__ == "__main__":
    main()
