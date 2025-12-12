import json
import os
import random
import time
from typing import Dict, Optional, Tuple

import requests

ESI_BASE = "https://esi.evetech.net/latest"
DATASOURCE = os.getenv("ESI_DATASOURCE", "tranquility")
USER_AGENT = os.getenv("USER_AGENT", "eve-esi-market-cloud-reader (github: eve-esi-market-cloud-reader)")
OUT_DIR = os.getenv("OUT_DIR", "out")
REGION_ID = os.getenv("REGION_ID")  # obligatorio

MIN_DELAY = float(os.getenv("MIN_DELAY", "0.15"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.35"))

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

def polite_sleep():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def read_error_limit(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[int]]:
    remain = headers.get("X-ESI-Error-Limit-Remain") or headers.get("x-esi-error-limit-remain")
    reset = headers.get("X-ESI-Error-Limit-Reset") or headers.get("x-esi-error-limit-reset")
    try:
        return (int(remain) if remain is not None else None, int(reset) if reset is not None else None)
    except ValueError:
        return (None, None)

def get_json(url: str, params: Dict[str, str]):
    r = session.get(url, params=params, timeout=45)

    # ESI error limiting (si baja mucho, frenamos)
    remain, reset = read_error_limit(r.headers)
    if remain is not None and remain <= 10:
        wait_s = (reset or 10) + 1
        print(f"[WARN] Error-limit bajo (remain={remain}). Pausando {wait_s}s.")
        time.sleep(wait_s)

    r.raise_for_status()
    return r.json(), dict(r.headers)

def main():
    if not REGION_ID:
        raise RuntimeError("Falta REGION_ID en variables de entorno.")

    region_id = int(REGION_ID)
    os.makedirs(OUT_DIR, exist_ok=True)
    pages_dir = os.path.join(OUT_DIR, f"orders_region_{region_id}")
    os.makedirs(pages_dir, exist_ok=True)

    # Descargar página 1 para saber X-Pages
    page = 1
    total_saved = 0

    while True:
        url = f"{ESI_BASE}/markets/{region_id}/orders/"
        params = {"datasource": DATASOURCE, "order_type": "all", "page": str(page)}
        data, headers = get_json(url, params)

        x_pages = headers.get("X-Pages") or headers.get("x-pages")
        if x_pages is None:
            raise RuntimeError("No llegó X-Pages; no puedo paginar con seguridad.")

        out_file = os.path.join(pages_dir, f"page_{page}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f)

        n = len(data)
        total_saved += n
        print(f"Region {region_id} page {page}/{x_pages}: guardadas {n} órdenes")

        if page >= int(x_pages):
            break

        page += 1
        polite_sleep()

    manifest = {
        "datasource": DATASOURCE,
        "region_id": region_id,
        "orders_saved": total_saved,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(os.path.join(OUT_DIR, f"manifest_region_{region_id}.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("DONE:", manifest)

if __name__ == "__main__":
    main()
