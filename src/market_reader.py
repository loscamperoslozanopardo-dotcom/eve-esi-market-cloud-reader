import json
import os
import random
import time
from typing import Dict, Optional, Tuple, List

import requests

ESI_BASE = "https://esi.evetech.net/latest"
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")

# Identificación clara (buena práctica)
USER_AGENT = os.getenv(
    "USER_AGENT",
    "eve-esi-market-cloud-reader (github: eve-esi-market-cloud-reader)"
)

# Modo “novato-friendly”: empezamos con pocas regiones por ejecución
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2"))
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))  # 0,1,2... para ir rotando lotes

# Ritmo conservador entre requests
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.15"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.35"))

OUT_DIR = os.getenv("OUT_DIR", "out")

session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
})

def polite_sleep():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def read_error_limit(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    remain = headers.get("X-ESI-Error-Limit-Remain") or headers.get("x-esi-error-limit-remain")
    reset = headers.get("X-ESI-Error-Limit-Reset") or headers.get("x-esi-error-limit-reset")
    try:
        return (int(remain) if remain is not None else None, int(reset) if reset is not None else None)
    except ValueError:
        return (None, None)

def load_json(path: str, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def get_json(url: str, params: Dict[str, str], etag_store: Dict[str, str]) -> Tuple[Optional[object], int, Dict[str, str]]:
    # Clave estable para ETag por URL+params
    key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    headers = {}

    # ETag / If-None-Match (ESI recomienda esto; 304 ahorra datos) :contentReference[oaicite:4]{index=4}
    if key in etag_store:
        headers["If-None-Match"] = etag_store[key]

    r = session.get(url, params=params, headers=headers, timeout=45)

    remain, reset = read_error_limit(r.headers)
    # Si el margen baja, nos frenamos (ESI expone estas cabeceras) :contentReference[oaicite:5]{index=5}
    if remain is not None and remain <= 10:
        wait_s = (reset or 10) + 1
        print(f"[WARN] Error-limit bajo (remain={remain}). Pausando {wait_s}s.")
        time.sleep(wait_s)

    if r.status_code == 304:
        return None, 304, dict(r.headers)

    r.raise_for_status()

    etag = r.headers.get("ETag") or r.headers.get("etag")
    if etag:
        etag_store[key] = etag

    return r.json(), r.status_code, dict(r.headers)

def chunk_regions(region_ids: List[int], batch_size: int, batch_index: int) -> List[int]:
    # Partimos la lista en lotes fijos, y elegimos el lote batch_index
    # Ej: batch_size=2 → [0..1], [2..3], [4..5]...
    start = batch_index * batch_size
    end = start + batch_size
    return region_ids[start:end]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "orders_pages"), exist_ok=True)

    etags_path = os.path.join(OUT_DIR, "etags.json")
    etag_store: Dict[str, str] = load_json(etags_path, {})

    # 1) Leer todas las regiones
    regions_url = f"{ESI_BASE}/universe/regions/"
    regions, st, _ = get_json(regions_url, {"datasource": DATASOURCE}, etag_store)
    if st == 304:
        # si 304, necesitamos cache previa para continuar
        regions_cache = load_json(os.path.join(OUT_DIR, "regions.json"), None)
        if regions_cache is None:
            raise RuntimeError("304 en /universe/regions/ y no hay cache local.")
        region_ids = regions_cache
    else:
        region_ids = regions
        save_json(os.path.join(OUT_DIR, "regions.json"), region_ids)

    region_ids = sorted(region_ids)
    print(f"Regiones totales: {len(region_ids)}")

    # 2) Elegimos un lote pequeño por ejecución (para ser responsables)
    batch_region_ids = chunk_regions(region_ids, BATCH_SIZE, BATCH_INDEX)
    if not batch_region_ids:
        print(f"No hay regiones en el lote BATCH_INDEX={BATCH_INDEX}. Fin.")
        return

    print(f"Lote actual (BATCH_INDEX={BATCH_INDEX}, BATCH_SIZE={BATCH_SIZE}): {batch_region_ids}")

    manifest = {
        "datasource": DATASOURCE,
        "batch_index": BATCH_INDEX,
        "batch_size": BATCH_SIZE,
        "regions_processed": [],
        "stats": {"requests_200": 0, "requests_304": 0, "orders_saved": 0},
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    for region_id in batch_region_ids:
        print(f"\n== Región {region_id} ==")
        page = 1
        region_orders = 0

        while True:
            orders_url = f"{ESI_BASE}/markets/{region_id}/orders/"
            params = {"datasource": DATASOURCE, "order_type": "all", "page": str(page)}
            data, st, headers = get_json(orders_url, params, etag_store)

            x_pages = headers.get("X-Pages") or headers.get("x-pages")
            if x_pages is None:
                # En general este endpoint se pagina; si no viene, no arriesgamos.
                raise RuntimeError("No llegó X-Pages; no puedo paginar con seguridad.")

            if st == 304:
                manifest["stats"]["requests_304"] += 1
                print(f"  page {page}/{x_pages}: 304")
            else:
                manifest["stats"]["requests_200"] += 1
                # Guardamos por página (solo para demostrar lectura en nube)
                out_file = os.path.join(OUT_DIR, "orders_pages", f"region_{region_id}_page_{page}.json")
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                n = len(data)
                region_orders += n
                manifest["stats"]["orders_saved"] += n
                print(f"  page {page}/{x_pages}: {n} órdenes guardadas")

            if page >= int(x_pages):
                break

            page += 1
            polite_sleep()

        manifest["regions_processed"].append({"region_id": region_id, "orders_saved": region_orders})
        print(f"Total región {region_id}: {region_orders}")

    save_json(etags_path, etag_store)
    save_json(os.path.join(OUT_DIR, "manifest.json"), manifest)

    print("\nMANIFEST:")
    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
