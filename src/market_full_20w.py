import gzip
import json
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, Any, Optional, List, Tuple

import requests

ESI_BASE = "https://esi.evetech.net/latest"
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")
USER_AGENT = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (full-20w)")

WORKERS = int(os.getenv("WORKERS", "20"))
OUT_DIR = os.getenv("OUT_DIR", "out")
STATE_FILE = os.getenv("STATE_FILE", "state/market_headers.json")

# En la primera ejecución no hay estado: bajamos todo.
# En runs posteriores, si Last-Modified no cambia, NO actualizamos región.
BOOTSTRAP = os.getenv("BOOTSTRAP", "1") == "1"

# Tu preferencia: “prefiero perder unos segundos a que el ETag diga que no se ha actualizado”
# => si Expires ya pasó y recibimos 304 en page=1, hacemos 1 verificación extra sin If-None-Match
FORCE_VERIFY_AFTER_EXPIRES = os.getenv("FORCE_VERIFY_AFTER_EXPIRES", "1") == "1"
INCONSISTENT_RETRY_ONCE = os.getenv("INCONSISTENT_RETRY_ONCE", "1") == "1"
INCONSISTENT_RETRY_MAX_PAGES = int(os.getenv("INCONSISTENT_RETRY_MAX_PAGES", "9999"))

# Control estricto de límites
ERROR_LIMIT_THRESHOLD = int(os.getenv("ERROR_LIMIT_THRESHOLD", "20"))  # si baja de aquí, pausa
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
TIMEOUT_S = int(os.getenv("TIMEOUT_S", "60"))

# Pausas pequeñas para no “martillear” y suavizar bursts (útil con rate limiting)
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.0"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.05"))

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

def sleep_small():
    if MAX_DELAY > 0:
        import random
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

_thread_local = threading.local()

def get_session() -> requests.Session:
    # Session por hilo (más seguro que compartirla)
    sess = getattr(_thread_local, "sess", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        _thread_local.sess = sess
    return sess

class GlobalThrottle:
    """
    Pausa GLOBAL para todos los workers cuando hay:
    - 429 con Retry-After (rate limiting)
    - error-limit bajo (X-ESI-Error-Limit-Remain/Reset)
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._pause_until = 0.0  # time.monotonic
        self.events: List[Dict[str, Any]] = []

    def pause(self, seconds: float, reason: str, meta: Optional[Dict[str, Any]] = None):
        until = time.monotonic() + max(0.0, seconds)
        with self._lock:
            if until > self._pause_until:
                self._pause_until = until
            self.events.append({
                "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reason": reason,
                "seconds": float(seconds),
                "meta": meta or {}
            })

    def wait_if_needed(self):
        while True:
            with self._lock:
                pause_until = self._pause_until
            now = time.monotonic()
            if now >= pause_until:
                return
            time.sleep(min(1.0, pause_until - now))

def read_error_limit(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    remain = headers.get("X-ESI-Error-Limit-Remain") or headers.get("x-esi-error-limit-remain")
    reset = headers.get("X-ESI-Error-Limit-Reset") or headers.get("x-esi-error-limit-reset")
    try:
        return (int(remain) if remain is not None else None,
                int(reset) if reset is not None else None)
    except ValueError:
        return (None, None)

def request_json(throttle: GlobalThrottle, url: str, params: Dict[str, str], headers: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, str], Any]:
    """
    Request robusto:
    - 429 => respeta Retry-After (pausa global) y reintenta
    - error-limit bajo => pausa global Reset+1
    - 5xx/timeouts => backoff con reintentos
    """
    sess = get_session()
    backoff = 1.0

    for attempt in range(1, MAX_RETRIES + 1):
        throttle.wait_if_needed()
        sleep_small()

        try:
            r = sess.get(url, params=params, headers=headers or {}, timeout=TIMEOUT_S)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff = min(20.0, backoff * 2)
            continue

        # Rate limit 429
        if r.status_code == 429:
            ra = r.headers.get("Retry-After") or r.headers.get("retry-after") or "5"
            try:
                wait_s = float(ra)
            except ValueError:
                wait_s = 5.0
            throttle.pause(wait_s + 0.5, "429 rate limited", {"retry_after": ra, "url": url})
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            continue

        # Error limit (ESI Best Practices)
        remain, reset = read_error_limit(r.headers)
        if remain is not None and remain <= ERROR_LIMIT_THRESHOLD:
            wait_s = float((reset or 10) + 1)
            throttle.pause(wait_s, "ESI error-limit low", {"remain": remain, "reset": reset})

        # 5xx retry
        if 500 <= r.status_code <= 599:
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(backoff)
            backoff = min(20.0, backoff * 2)
            continue

        # Otros 4xx (excepto 304 que tratamos fuera) => no reintentar en bucle
        if 400 <= r.status_code <= 499 and r.status_code not in (304,):
            r.raise_for_status()

        hdrs = {k: v for k, v in r.headers.items()}
        if r.status_code == 304:
            return 304, hdrs, None

        return r.status_code, hdrs, r.json()

    raise RuntimeError("request_json: unreachable")

@dataclass
class RegionScan:
    region_id: int
    status: int
    etag: Optional[str]
    expires: Optional[str]
    last_modified: Optional[str]
    x_pages: int
    page1_data: Optional[Any]
    should_fetch: bool
    reason: str

@dataclass
class RegionResult:
    region_id: int
    ok: bool
    pages: int
    orders: int
    last_modified: str
    retried: bool = False
    error: Optional[str] = None
    tmp_path: Optional[str] = None

def get_regions(throttle: GlobalThrottle) -> List[int]:
    url = f"{ESI_BASE}/universe/regions/"
    st, hdrs, data = request_json(throttle, url, {"datasource": DATASOURCE})
    return sorted(data)

def scan_region(throttle: GlobalThrottle, state: Dict[str, Any], region_id: int) -> RegionScan:
    url = f"{ESI_BASE}/markets/{region_id}/orders/"
    params = {"datasource": DATASOURCE, "order_type": "all", "page": "1"}

    entry = state.setdefault("regions", {}).setdefault(str(region_id), {})
    prev_etag = entry.get("etag_page1")
    prev_lm = entry.get("last_modified")

    headers = {}
    if prev_etag:
        headers["If-None-Match"] = prev_etag

    status, hdrs, data = request_json(throttle, url, params, headers=headers)

    etag = hdrs.get("ETag") or hdrs.get("etag")
    expires = hdrs.get("Expires") or hdrs.get("expires")
    last_modified = hdrs.get("Last-Modified") or hdrs.get("last-modified")
    x_pages_raw = hdrs.get("X-Pages") or hdrs.get("x-pages") or "1"
    try:
        x_pages = int(x_pages_raw)
    except ValueError:
        x_pages = 1

    # Si es 304, normalmente no hay body; consideramos LM como el previo
    if status == 304:
        last_modified = last_modified or prev_lm

    # Guardamos estado de headers (ETag Best Practices: guardar ETag independiente de expiry)
    if etag:
        entry["etag_page1"] = etag
    if expires:
        entry["expires"] = expires
    if last_modified:
        entry["last_modified"] = last_modified
    entry["x_pages"] = x_pages

    exp_dt = parse_http_date(expires)
    expired = (exp_dt is None) or (now_utc() >= exp_dt)

    has_snapshot = bool(entry.get("last_full_snapshot_utc"))

    # Decisión: si LM no cambia, no refrescamos (tu requisito)
    changed = (not has_snapshot) or (prev_lm is None) or (last_modified is None) or (last_modified != prev_lm)

    # BOOTSTRAP: si no hay snapshot previo, sí o sí fetch
    if BOOTSTRAP and (not has_snapshot):
        should_fetch = True
        reason = "bootstrap (no snapshot previo)"
    else:
        # Respetuoso: si no expiró y no cambió LM => skip
        if (not expired) and (not changed):
            should_fetch = False
            reason = "not expired + Last-Modified unchanged"
        else:
            should_fetch = changed
            reason = "changed (Last-Modified)" if changed else "unchanged"

    page1_data = data if status == 200 else None

    # Si ya expiró y recibimos 304, y tú quieres “verificar aunque cueste”:
    # hacemos que el worker vuelva a pedir page=1 sin If-None-Match (para obtener body y LM seguro)
    if should_fetch and expired and status == 304 and FORCE_VERIFY_AFTER_EXPIRES:
        page1_data = None  # fuerza fetch de page1 sin conditional en fase de descarga

    return RegionScan(
        region_id=region_id,
        status=status,
        etag=etag,
        expires=expires,
        last_modified=last_modified,
        x_pages=x_pages,
        page1_data=page1_data,
        should_fetch=should_fetch,
        reason=reason,
    )

def lpt_assign(regions: List[RegionScan], workers: int) -> List[List[RegionScan]]:
    # Longest Processing Time first: balance por páginas
    bins = [[] for _ in range(workers)]
    load = [0] * workers
    for r in sorted(regions, key=lambda x: x.x_pages, reverse=True):
        i = min(range(workers), key=lambda j: load[j])
        bins[i].append(r)
        load[i] += r.x_pages
    return bins

def fetch_region(throttle: GlobalThrottle, state: Dict[str, Any], scan: RegionScan, tmp_dir: str) -> RegionResult:
    region_id = scan.region_id
    url = f"{ESI_BASE}/markets/{region_id}/orders/"
    tmp_path = os.path.join(tmp_dir, f"region_{region_id}.jsonl")

    def _cleanup_tmp():
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

    def _download_with_page1(page1_data, x_pages: int, lm_ref: str) -> RegionResult:
        orders_count = 0
        with open(tmp_path, "w", encoding="utf-8") as f:
            # page 1
            for o in page1_data:
                o["region_id"] = region_id
                f.write(json.dumps(o, separators=(",", ":"), ensure_ascii=False) + "\n")
            orders_count += len(page1_data)

            # pages 2..N secuencial
            for page in range(2, x_pages + 1):
                st, hdrs, data = request_json(
                    throttle, url,
                    {"datasource": DATASOURCE, "order_type": "all", "page": str(page)},
                    headers={}
                )
                lm = (hdrs.get("Last-Modified") or hdrs.get("last-modified") or "")
                if lm_ref and lm and lm != lm_ref:
                    raise RuntimeError(f"Inconsistent snapshot: Last-Modified changed (ref={lm_ref}, got={lm})")

                for o in data:
                    o["region_id"] = region_id
                    f.write(json.dumps(o, separators=(",", ":"), ensure_ascii=False) + "\n")
                orders_count += len(data)

        # Éxito: solo aquí actualizamos baseline “snapshot last modified”
        entry = state.setdefault("regions", {}).setdefault(str(region_id), {})
        entry["last_full_snapshot_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        entry["last_snapshot_last_modified"] = lm_ref or entry.get("last_modified_seen") or ""

        return RegionResult(region_id=region_id, ok=True, pages=x_pages, orders=orders_count, last_modified=lm_ref, tmp_path=tmp_path)

    # Intento 0 y (si procede) intento 1
    attempts = 2 if INCONSISTENT_RETRY_ONCE else 1

    last_err = None
    for attempt in range(attempts):
        try:
            _cleanup_tmp()

            # Asegurar page1 + headers (si scan no trae body o queremos verificar)
            if scan.page1_data is None or attempt == 1:
                st, hdrs, page1 = request_json(
                    throttle, url,
                    {"datasource": DATASOURCE, "order_type": "all", "page": "1"},
                    headers={}
                )
                x_pages_raw = hdrs.get("X-Pages") or hdrs.get("x-pages") or str(scan.x_pages)
                try:
                    x_pages = int(x_pages_raw)
                except ValueError:
                    x_pages = scan.x_pages
                lm_ref = (hdrs.get("Last-Modified") or hdrs.get("last-modified") or scan.last_modified or "")
                # actualizar “seen” (no baseline)
                entry = state.setdefault("regions", {}).setdefault(str(region_id), {})
                if lm_ref:
                    entry["last_modified_seen"] = lm_ref
                scan_x_pages = x_pages
                page1_data = page1
            else:
                scan_x_pages = scan.x_pages
                page1_data = scan.page1_data
                lm_ref = scan.last_modified or ""

            # Si es enorme y quieres evitar dobles descargas, puedes limitar el retry por páginas:
            if attempt == 1 and scan_x_pages > INCONSISTENT_RETRY_MAX_PAGES:
                raise RuntimeError(f"Inconsistent snapshot (retry skipped: pages={scan_x_pages} > max={INCONSISTENT_RETRY_MAX_PAGES})")

            return _download_with_page1(page1_data, scan_x_pages, lm_ref)

        except Exception as e:
            last_err = str(e)
            # Solo reintentar si es inconsistencia y aún queda intento
            if ("Inconsistent snapshot" in last_err) and (attempt == 0) and INCONSISTENT_RETRY_ONCE:
                # reintento inmediato 1 vez
                continue
            break

    _cleanup_tmp()
    entry = state.setdefault("regions", {}).setdefault(str(region_id), {})
    entry["last_error"] = last_err or "unknown error"
    return RegionResult(region_id=region_id, ok=False, pages=scan.x_pages, orders=0, last_modified=scan.last_modified or "", error=last_err, tmp_path=None)

def merge_to_single_file(tmp_dir: str, out_path_gz: str, results: List[RegionResult]) -> Tuple[int, int]:
    ok_regions = [r for r in results if r.ok and r.tmp_path]
    # Orden estable por region_id (para reproducibilidad)
    ok_regions.sort(key=lambda r: r.region_id)

    out_tmp = out_path_gz + ".tmp"
    total_orders = 0
    total_regions = 0

    with gzip.open(out_tmp, "wt", encoding="utf-8") as gz:
        for rr in ok_regions:
            total_regions += 1
            total_orders += rr.orders
            with open(rr.tmp_path, "r", encoding="utf-8") as f:
                shutil.copyfileobj(f, gz)

    os.replace(out_tmp, out_path_gz)
    return total_regions, total_orders

def main():
    t0 = time.time()
    os.makedirs(OUT_DIR, exist_ok=True)
    tmp_dir = os.path.join(OUT_DIR, "tmp_regions")
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    throttle = GlobalThrottle()
    state = load_json(STATE_FILE, {"regions": {}})

    regions = get_regions(throttle)

    # 1) Scan inicial page=1 de todas las regiones (para X-Pages + ETag/LM/Expires)
    scans: List[RegionScan] = []
    scan_errors = 0

    def _scan(rid: int) -> RegionScan:
        return scan_region(throttle, state, rid)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_scan, rid): rid for rid in regions}
        for fut in as_completed(futs):
            rid = futs[fut]
            try:
                scans.append(fut.result())
            except Exception as e:
                scan_errors += 1
                state.setdefault("regions", {}).setdefault(str(rid), {})["last_error"] = f"scan failed: {e}"

    to_fetch = [s for s in scans if s.should_fetch]
    skipped = [s for s in scans if not s.should_fetch]

    # 2) Reparto por páginas para balancear workers (LPT)
    bins = lpt_assign(to_fetch, WORKERS)

    results: List[RegionResult] = []
    results_lock = threading.Lock()

    def worker_run(bin_scans: List[RegionScan]):
        local = []
        for s in bin_scans:
            local.append(fetch_region(throttle, state, s, tmp_dir))
        with results_lock:
            results.extend(local)

    # 3) 20 workers “región completa” (secuencial por páginas dentro de región)
    threads = []
    for b in bins:
        th = threading.Thread(target=worker_run, args=(b,), daemon=True)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

    # 4) Merge final a un único archivo
    out_file = os.path.join(OUT_DIR, "market_orders_all.jsonl.gz")
    regions_ok, orders_ok = merge_to_single_file(tmp_dir, out_file, results)

    # Limpieza de temporales (dejas solo el archivo único final)
    shutil.rmtree(tmp_dir, ignore_errors=True)

    # Guardar estado mínimo para el siguiente run (ETag/LM/Expires/x_pages + last_full_snapshot_utc)
    save_json(STATE_FILE, state)

    # Manifest
    failed = [r for r in results if not r.ok]
    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions_total": len(regions),
        "scan_errors": scan_errors,
        "regions_skipped": len(skipped),
        "regions_to_fetch": len(to_fetch),
        "regions_ok": regions_ok,
        "regions_failed": len(failed),
        "orders_ok": orders_ok,
        "seconds": round(time.time() - t0, 3),
        "workers": WORKERS,
        "throttle_events": throttle.events[:50],  # primeras 50 (si hay muchas)
        "failed_regions": [{"region_id": r.region_id, "error": r.error} for r in failed[:20]],
        "output_file": "market_orders_all.jsonl.gz",
        "notes": {
            "one_region_one_worker": True,
            "single_final_file": True,
            "strict_429_retry_after": True,
            "strict_error_limit_pause": True,
        }
    }
    save_json(os.path.join(OUT_DIR, "manifest.json"), manifest)

    print(json.dumps(manifest, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
