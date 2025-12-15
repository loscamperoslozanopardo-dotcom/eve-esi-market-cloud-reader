import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional

import requests


ESI_BASE = "https://esi.evetech.net/latest"
PRICES_PATH = "/markets/prices/"


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


def _read_int(headers: Dict[str, str], name: str) -> Optional[int]:
    v = headers.get(name) or headers.get(name.lower())
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def _session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip",
        }
    )
    return s


def fetch_markets_prices(
    prev_state: Dict[str, Any],
    datasource: str,
    user_agent: str,
    timeout_s: int,
    max_retries: int,
    error_limit_threshold: int,
) -> Dict[str, Any]:
    """
    Return:
      {
        "status": int,
        "headers": dict,
        "data": Any | None
      }
    """
    url = f"{ESI_BASE}{PRICES_PATH}"
    sess = _session(user_agent)

    # Conditional headers
    req_headers: Dict[str, str] = {}
    prev_etag = prev_state.get("etag")
    if isinstance(prev_etag, str) and prev_etag:
        req_headers["If-None-Match"] = prev_etag

    prev_lm = prev_state.get("last_modified")
    if isinstance(prev_lm, str) and prev_lm:
        req_headers["If-Modified-Since"] = prev_lm

    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            r = sess.get(
                url,
                params={"datasource": datasource},
                headers=req_headers,
                timeout=timeout_s,
            )
        except requests.RequestException as e:
            if attempt == max_retries:
                raise RuntimeError(f"ESI request failed after {max_retries} attempts: {e}") from e
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

        # ESI error limit protection
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
            return {"status": 304, "headers": hdrs, "data": None}

        return {"status": r.status_code, "headers": hdrs, "data": r.json()}

    raise RuntimeError("unreachable: retries loop exhausted")
