import json
import os
import time
import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, Any, Optional, Tuple, List
import requests

ESI_BASE = "https://esi.evetech.net/latest"
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")
USER_AGENT = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (serious-5min)")
OUT_DIR = os.getenv("OUT_DIR", "out")
STATE_PATH = os.getenv("STATE_PATH", "state/market_state.json")

# Concurrencia / carga (ajustable)
MAX_REGIONS_PER_RUN = int(os.getenv("MAX_REGIONS_PER_RUN", "6"))  # sube/baja según veas tiempos
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.10"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.25"))

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

def sleep_polite():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

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

def read_error_limit(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    remain = headers.get("X-ESI-Error-Limit-Remain") or headers.get("x-esi-error-limit-remain")
    reset = headers.get("X-ESI-Error-Limit-Reset") or headers.get("x-esi-error-limit-reset")
    try:
        return (int(remain) if remain is not None else None,
                int(reset) if reset is not None else None)
    except ValueError:
        return (None, None)

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

def head_page1(region_id: int, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Revisión rápida: siempre pedimos page=1 para leer Expires/Last-Modified/ETag/X-Pages.
    Tu preferencia: “prefiero perder unos segundos a que el ETag diga que no se ha actualizado”.
    => Cuando creemos que toca refrescar, haremos GET sin If-None-Match (forzar 200 tras Expires).
    """
    url = f"{ESI_BASE}/markets/{region_id}/orders/"
    params = {"datasource": DATASOURCE, "order_type": "all", "page": "1"}

    # Para la fase de "revisión", sí usamos If-None-Match si lo tenemos: es barato.
    headers = {}
    etag = state.get("regions", {}).get(str(region_id), {}).get("etag_page1")
    if etag:
        headers["If-None-Match"] = etag

    r = session.get(url, params=params, headers=headers, timeout=45)

    remain, reset = read_error_limit(r.headers)
    if remain is not None and remain <= 10:
        wait_s = (reset or 10) + 1
        print(f"[WARN] Error-limit bajo (remain={remain}). Sleep {wait_s}s")
        time.sleep(wait_s)

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
    # Devolvemos también los datos de page1 para reutilizarlos si hacemos snapshot completo
    info["page1_data"] = r.json()
    return info

def fetch_region_snapshot(region_id: int, page1_data: Any, x_pages: int, lm_ref: str) -> Dict[str, Any]:
    """
    Descarga completa "lo antes posible" tras update.
    - No usamos If-None-Match aquí: queremos 200 y consistencia del snapshot.
    - Validación: Last-Modified igual en todas las páginas (recomendación ESI para recursos paginados).
    """
    region_dir = os.path.join(OUT_DIR, "orders", f"region_{region_id}")
    os.makedirs(region_dir, exist_ok=True)

    # Guardar page 1
    with open(os.path.join(region_dir, "page_1.json"), "w", encoding="utf-8") as f:
        json.dump(page1_data, f)

    total = len(page1_data)

    # Resto de páginas
    for page in range(2, x_pages + 1):
        url = f"{ESI_BASE}/markets/{region_id}/orders/"
        params = {"datasource": DATASOURCE, "order_type": "all", "page": str(page)}
        r = session.get(url, params=params, timeout=60)
        remain, reset = read_error_limit(r.headers)
        if remain is not None and remain <= 10:
            wait_s = (reset or 10) + 1
            print(f"[WARN] Error-limit bajo (remain={remain}). Sleep {wait_s}s")
            time.sleep(wait_s)

        r.raise_for_status()
        lm = (r.headers.get("Last-Modified") or r.headers.get("last-modified") or "")
        if lm_ref and lm and (lm != lm_ref):
            # snapshot inconsistente: abortamos (mejor repetir post-Expires)
            raise RuntimeError(f"Snapshot inconsistente en región {region_id}: Last-Modified cambió (ref={lm_ref}, got={lm})")

        data = r.json()
        total += len(data)
        with open(os.path.join(region_dir, f"page_{page}.json"), "w", encoding="utf-8") as f:
            json.dump(data, f)

        sleep_polite()

    return {"region_id": region_id, "pages": x_pages, "orders": total, "last_modified": lm_ref}

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    state = load_json(STATE_PATH, {"regions": {}})
    regions = get_regions()

    # Priorización simple: regiones con expires más antiguo primero.
    def region_priority(rid: int) -> float:
        entry = state["regions"].get(str(rid), {})
        exp = parse_http_date(entry.get("expires"))
        return exp.timestamp() if exp else 0.0

    regions_sorted = sorted(regions, key=region_priority)

    picked = []
    snapshots = []
    t0 = time.time()

    for region_id in regions_sorted:
        if len(picked) >= MAX_REGIONS_PER_RUN:
            break

        info = head_page1(region_id, state)

        # Actualizamos estado con headers nuevos aunque sea 304 (Expires puede cambiar)
        entry = state["regions"].setdefault(str(region_id), {})
        if info.get("etag"):
            entry["etag_page1"] = info["etag"]
        if info.get("expires"):
            entry["expires"] = info["expires"]
        if info.get("last_modified"):
            entry["last_modified"] = info["last_modified"]
        if info.get("x_pages"):
            entry["x_pages"] = info["x_pages"]

        exp_dt = parse_http_date(info.get("expires"))
        # Si aún no ha expirado, no hacemos full snapshot (evita “circumventing cache”)
        if exp_dt and now_utc() < exp_dt:
            continue

        # Si 304 justo después de Expires, pedimos 200 “a propósito”
        # (tu preferencia: perder segundos > fiarte del 304 en este punto)
        if info["status"] == 304:
            url = f"{ESI_BASE}/markets/{region_id}/orders/"
            params = {"datasource": DATASOURCE, "order_type": "all", "page": "1"}
            r = session.get(url, params=params, timeout=45)  # sin If-None-Match
            r.raise_for_status()
            page1_data = r.json()
            lm_ref = (r.headers.get("Last-Modified") or r.headers.get("last-modified") or "")
            x_pages = int((r.headers.get("X-Pages") or r.headers.get("x-pages") or "1"))
        else:
            page1_data = info.get("page1_data", [])
            lm_ref = info.get("last_modified") or ""
            x_pages = int(info.get("x_pages") or "1")

        picked.append(region_id)

        try:
            snap = fetch_region_snapshot(region_id, page1_data, x_pages, lm_ref)
            snapshots.append(snap)
            entry["last_full_snapshot_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            entry["last_error"] = str(e)
            print(f"[ERROR] Región {region_id}: {e}")

    # Manifest del run
    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions_total": len(regions),
        "regions_picked": picked,
        "snapshots": snapshots,
        "seconds": round(time.time() - t0, 3),
        "note": "Este run solo refresca un subconjunto (MAX_REGIONS_PER_RUN) para poder cumplir cada 5 min sin saturar.",
    }
    save_json(os.path.join(OUT_DIR, "manifest.json"), manifest)
    save_json(STATE_PATH, state)

    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
