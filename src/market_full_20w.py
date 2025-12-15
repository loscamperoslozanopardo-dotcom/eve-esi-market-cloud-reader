import os
import json
import time
import gzip
import shutil
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter

ESI_BASE = "https://esi.evetech.net/latest"

# -------------------------
# Config (env)
# -------------------------
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")
USER_AGENT = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (github actions)")
OUT_DIR = os.getenv("OUT_DIR", "out")
STATE_FILE = os.getenv("STATE_FILE", os.path.join(OUT_DIR, "state", "market_headers.json"))

WORKERS = int(os.getenv("WORKERS", "20"))

BOOTSTRAP = os.getenv("BOOTSTRAP", "0") == "1"
FORCE_VERIFY_AFTER_EXPIRES = os.getenv("FORCE_VERIFY_AFTER_EXPIRES", "1") == "1"
FORCE_FULL_SNAPSHOT = os.getenv("FORCE_FULL_SNAPSHOT", "0") == "1"

ERROR_LIMIT_THRESHOLD = int(os.getenv("ERROR_LIMIT_THRESHOLD", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
TIMEOUT_S = int(os.getenv("TIMEOUT_S", "60"))

# Inconsistencias (LM cambia dentro de una misma región mientras descargas páginas)
INCONSISTENT_RETRY_ONCE = True

# Rate-limit bucket protection (tokens)
# Si Remaining baja mucho, pausamos un poco antes de provocar 429.
RATELIMIT_SOFT_THRESHOLD = int(os.getenv("RATELIMIT_SOFT_THRESHOLD", "10"))
RATELIMIT_SOFT_SLEEP_S = float(os.getenv("RATELIMIT_SOFT_SLEEP_S", "0.8"))

# Micro-jitter para evitar “thundering herd”
JITTER_MAX_S = float(os.getenv("JITTER_MAX_S", "0.12"))

_session = None
_session_lock = threading.Lock()

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_http_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def sleep_small():
    if JITTER_MAX_S > 0:
        time.sleep(random.random() * JITTER_MAX_S)

def get_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    with _session_lock:
        if _session is not None:
            return _session
        s = requests.Session()
        s.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip",
        })
        # Pool más grande para 20 workers
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _session = s
        return _session

def read_error_limit(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    # ESI error limit headers: X-ESI-Error-Limit-Remain / Reset
    def _get(h: str) -> Optional[str]:
        return headers.get(h) or headers.get(h.lower())

    remain_s = _get("X-ESI-Error-Limit-Remain")
    reset_s = _get("X-ESI-Error-Limit-Reset")
    remain = None
    reset = None
    try:
        if remain_s is not None:
            remain = int(remain_s)
    except Exception:
        pass
    try:
        if reset_s is not None:
            reset = int(float(reset_s))
    except Exception:
        pass
    return remain, reset

def read_bucket_remaining(headers: Dict[str, str]) -> Optional[int]:
    # ESI bucket rate limit header: X-Ratelimit-Remaining
    val = headers.get("X-Ratelimit-Remaining") or headers.get("x-ratelimit-remaining")
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        return None

class GlobalThrottle:
    def __init__(self):
        self._lock = threading.Lock()
        self._pause_until = 0.0
        self.events: List[Dict[str, Any]] = []

    def pause(self, seconds: float, reason: str, meta: Optional[Dict[str, Any]] = None):
        until = time.time() + max(0.0, seconds)
        with self._lock:
            if until > self._pause_until:
                self._pause_until = until
            evt = {"t": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"), "pause_s": round(seconds, 3), "reason": reason}
            if meta:
                evt["meta"] = meta
            self.events.append(evt)

    def wait_if_needed(self):
        while True:
            with self._lock:
                remaining = self._pause_until - time.time()
            if remaining <= 0:
                return
            time.sleep(min(0.5, remaining))

def request_json(throttle: GlobalThrottle, url: str, params: Dict[str, str], headers: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, str], Any]:
    """
    Request robusto:
    - 429 => respeta Retry-After (pausa global) y reintenta
    - error-limit bajo => pausa global Reset+1
    - bucket rate-limit bajo => micro-pausa preventiva (evita 429)
    - 5xx/timeouts => backoff con reintentos
    """
    sess = get_session()
    backoff = 1.0

    for attempt in range(1, MAX_RETRIES + 1):
        throttle.wait_if_needed()
        sleep_small()

        try:
            r = sess.get(url, params=params, headers=headers or {}, timeout=TIMEOUT_S)
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff = min(10.0, backoff * 2)
            continue

        hdrs = {k: v for k, v in r.headers.items()}

        # 429: obey Retry-After
        if r.status_code == 429:
            ra = r.headers.get("Retry-After") or r.headers.get("retry-after") or "5"
            try:
                wait_s = float(ra)
            except Exception:
                wait_s = 5.0
            throttle.pause(wait_s + 0.5, "429 rate limited", {"retry_after": ra, "url": url})
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            continue

        # Error limit protection (global ban protection)
        remain, reset = read_error_limit(r.headers)
        if remain is not None and remain <= ERROR_LIMIT_THRESHOLD:
            wait_s = float((reset or 10) + 1)
            throttle.pause(wait_s, "ESI error-limit low", {"remain": remain, "reset": reset})

        # Bucket protection: if tokens low, slow down a bit
        bucket_rem = read_bucket_remaining(r.headers)
        if bucket_rem is not None and bucket_rem <= RATELIMIT_SOFT_THRESHOLD:
            throttle.pause(RATELIMIT_SOFT_SLEEP_S, "ESI bucket low (soft)", {"remaining": bucket_rem})

        # 5xx retry
        if 500 <= r.status_code <= 599:
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(backoff)
            backoff = min(10.0, backoff * 2)
            continue

        # Other 4xx (except 304)
        if 400 <= r.status_code <= 499 and r.status_code not in (304,):
            r.raise_for_status()

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

    # Migración suave si vienes de estado antiguo
    if entry.get("last_full_snapshot_utc") and entry.get("last_modified") and not entry.get("last_snapshot_last_modified"):
        entry["last_snapshot_last_modified"] = entry["last_modified"]

    prev_etag = entry.get("etag_page1")
    prev_seen_lm = entry.get("last_modified_seen")
    last_snap_lm = entry.get("last_snapshot_last_modified")
    has_snapshot = bool(entry.get("last_full_snapshot_utc"))

    headers = {}
    if prev_etag:
        headers["If-None-Match"] = prev_etag  # ETag best practices

    status, hdrs, data = request_json(throttle, url, params, headers=headers)

    etag = hdrs.get("ETag") or hdrs.get("etag")
    expires = hdrs.get("Expires") or hdrs.get("expires")
    last_modified = hdrs.get("Last-Modified") or hdrs.get("last-modified")
    x_pages_raw = hdrs.get("X-Pages") or hdrs.get("x-pages") or "1"
    try:
        x_pages = int(x_pages_raw)
    except Exception:
        x_pages = 1

    # 304 puede venir sin LM
    if status == 304 and not last_modified:
        last_modified = prev_seen_lm

    # Guardar estado de headers
    if etag:
        entry["etag_page1"] = etag
    if expires:
        entry["expires"] = expires
    if last_modified:
        entry["last_modified_seen"] = last_modified
    entry["x_pages"] = x_pages

    exp_dt = parse_http_date(expires)
    expired = (exp_dt is None) or (now_utc() >= exp_dt)

    # Decidir si hay que descargar región
    should_fetch = False
    reason = "skip"

    if FORCE_FULL_SNAPSHOT or not has_snapshot:
        should_fetch = True
        reason = "no_snapshot_or_forced"
    else:
        # Si no ha expirado, lo normal es “no fetch”.
        # Pero si FORCE_VERIFY_AFTER_EXPIRES=1, solo re-verificamos cuando expire.
        if expired:
            # Si ya tenemos snapshot y LM del snapshot coincide con LM actual, no hace falta refetch.
            # (ETag/304 ya nos ayuda aquí)
            if last_snap_lm and last_modified and last_modified == last_snap_lm:
                should_fetch = False
                reason = "expired_but_same_last_modified"
            else:
                should_fetch = True
                reason = "expired_and_changed_or_unknown"
        else:
            should_fetch = False
            reason = "not_expired"

    # BOOTSTRAP: en el primer run, fuerzas todo
    if BOOTSTRAP:
        should_fetch = True
        reason = "bootstrap"

    page1_data = None
    if status != 304:
        page1_data = data

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

            # pages 2..N (secuencial dentro de región)
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

        # Éxito: solo aquí “sellamos” snapshot
        entry = state.setdefault("regions", {}).setdefault(str(region_id), {})
        entry["last_full_snapshot_utc"] = now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
        if lm_ref:
            entry["last_snapshot_last_modified"] = lm_ref
        elif entry.get("last_modified_seen"):
            entry["last_snapshot_last_modified"] = entry["last_modified_seen"]

        return RegionResult(region_id=region_id, ok=True, pages=x_pages, orders=orders_count, last_modified=lm_ref, tmp_path=tmp_path)

    did_retry = False
    last_err: Optional[str] = None
    attempts = 2 if INCONSISTENT_RETRY_ONCE else 1

    for attempt in range(attempts):
        try:
            _cleanup_tmp()

            # Asegurar page1
            if scan.page1_data is None or attempt == 1:
                st, hdrs, page1 = request_json(
                    throttle, url,
                    {"datasource": DATASOURCE, "order_type": "all", "page": "1"},
                    headers={}
                )
                lm_ref = (hdrs.get("Last-Modified") or hdrs.get("last-modified") or "")
                x_pages_raw = hdrs.get("X-Pages") or hdrs.get("x-pages") or "1"
                try:
                    x_pages = int(x_pages_raw)
                except Exception:
                    x_pages = 1
                page1_data = page1
            else:
                page1_data = scan.page1_data
                lm_ref = scan.last_modified or ""
                x_pages = scan.x_pages

            rr = _download_with_page1(page1_data, x_pages, lm_ref)
            rr.retried = did_retry
            return rr

        except Exception as e:
            last_err = str(e)
            # Guardar error para inspección
            state.setdefault("regions", {}).setdefault(str(region_id), {})["last_error"] = last_err
            if "Inconsistent snapshot" in last_err and attempt == 0 and attempts == 2:
                did_retry = True
                # mini backoff antes del reintento
                time.sleep(0.6)
                continue
            _cleanup_tmp()
            return RegionResult(region_id=region_id, ok=False, pages=scan.x_pages, orders=0, last_modified=scan.last_modified or "", retried=did_retry, error=last_err)

    _cleanup_tmp()
    return RegionResult(region_id=region_id, ok=False, pages=scan.x_pages, orders=0, last_modified=scan.last_modified or "", retried=did_retry, error=last_err)

def merge_to_single_file(tmp_dir: str, out_path_gz: str, results: List[RegionResult]) -> Tuple[int, int]:
    ok_regions = [r for r in results if r.ok and r.tmp_path]
    ok_regions.sort(key=lambda r: r.region_id)

    out_tmp = out_path_gz + ".tmp"
    total_orders = 0
    total_regions = 0

    # compresslevel=1 => mucho más rápido (y suficiente para BQ)
    with gzip.open(out_tmp, "wt", encoding="utf-8", compresslevel=1) as gz:
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

    # 1) Scan inicial page=1 de todas las regiones
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

    # 2) Balanceo LPT
    bins = lpt_assign(to_fetch, WORKERS)

    results: List[RegionResult] = []
    results_lock = threading.Lock()

    def worker_run(bin_scans: List[RegionScan]):
        local = []
        for s in bin_scans:
            local.append(fetch_region(throttle, state, s, tmp_dir))
        with results_lock:
            results.extend(local)

    # 3) Workers por región
    threads = []
    for b in bins:
        th = threading.Thread(target=worker_run, args=(b,), daemon=True)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

    # 4) Merge final SOLO si hay regiones descargadas
    if not results:
        manifest = {
            "timestamp_utc": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "regions_total": len(regions),
            "scan_errors": scan_errors,
            "regions_skipped": len(skipped),
            "regions_to_fetch": 0,
            "regions_ok": 0,
            "orders_ok": 0,
            "seconds": round(time.time() - t0, 3),
            "note": "No changes detected; previous snapshot kept",
        }
        save_json(os.path.join(OUT_DIR, "manifest.json"), manifest)
        save_json(STATE_FILE, state)
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
        return

    out_file = os.path.join(OUT_DIR, "market_orders_all.jsonl.gz")
    regions_ok, orders_ok = merge_to_single_file(tmp_dir, out_file, results)

    shutil.rmtree(tmp_dir, ignore_errors=True)
    save_json(STATE_FILE, state)

    failed = [r for r in results if not r.ok]
    retried_count = sum(1 for r in results if getattr(r, "retried", False))
    manifest = {
        "timestamp_utc": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions_total": len(regions),
        "scan_errors": scan_errors,
        "regions_skipped": len(skipped),
        "regions_to_fetch": len(to_fetch),
        "regions_ok": regions_ok,
        "regions_failed": len(failed),
        "regions_retried": retried_count,
        "orders_ok": orders_ok,
        "seconds": round(time.time() - t0, 3),
        "workers": WORKERS,
        "throttle_events": throttle.events[:50],
        "failed_regions": [{"region_id": r.region_id, "error": r.error} for r in failed[:20]],
        "output_file": "market_orders_all.jsonl.gz",
        "notes": {
            "one_region_one_worker": True,
            "single_final_file": True,
            "strict_429_retry_after": True,
            "strict_error_limit_pause": True,
            "soft_bucket_throttle": True,
            "gzip_fast": True,
        },
    }
    save_json(os.path.join(OUT_DIR, "manifest.json"), manifest)
    print(json.dumps(manifest, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
