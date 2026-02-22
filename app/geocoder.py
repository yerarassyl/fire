import os
import time
import requests
from typing import Optional, Tuple
from app.db import get_conn


def _base_url() -> str:
    return os.getenv("DGIS_BASE_URL", "https://catalog.api.2gis.com").rstrip("/")


def _timeout() -> int:
    try:
        return int(os.getenv("DGIS_TIMEOUT", "15"))
    except:
        return 15


def _retries() -> int:
    try:
        return int(os.getenv("DGIS_RETRIES", "2"))
    except:
        return 2


def _api_key() -> str:
    key = os.getenv("DGIS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("DGIS_API_KEY is not set in .env")
    return key


def geocode_address_precise(address: str) -> Optional[Tuple[float, float]]:
    """
    2GIS forward geocoding (максимальная точность по полному адресу).
    Использует кэш в PostgreSQL (geocode_cache).
    Endpoint: /3.0/items/geocode?q=...&fields=items.point&key=...  :contentReference[oaicite:2]{index=2}
    Returns (lon, lat) or None
    """
    if not address or not address.strip():
        return None

    q = " ".join(address.split()).strip()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT lon, lat, status FROM geocode_cache WHERE query_text=%s", (q,))
        row = cur.fetchone()
        if row:
            lon, lat, status = row
            if status == "OK" and lon is not None and lat is not None:
                return float(lon), float(lat)
            if status == "NOT_FOUND":
                return None

        url = f"{_base_url()}/3.0/items/geocode"
        params = {
            "q": q,
            "fields": "items.point",
            "key": _api_key(),
        }

        last_err = None
        for attempt in range(_retries() + 1):
            try:
                r = requests.get(url, params=params, timeout=_timeout())
                r.raise_for_status()
                data = r.json()

                # 2GIS обычно возвращает {"result":{"items":[{"point":{"lon":..,"lat":..}}]}}
                items = (((data or {}).get("result") or {}).get("items")) or []
                if not items:
                    cur.execute(
                        """
                        INSERT INTO geocode_cache(query_text, status, provider)
                        VALUES (%s, 'NOT_FOUND', '2gis')
                        ON CONFLICT (query_text) DO UPDATE
                          SET status='NOT_FOUND', provider='2gis', updated_at=NOW()
                        """,
                        (q,),
                    )
                    conn.commit()
                    return None

                point = items[0].get("point") or {}
                lon = point.get("lon")
                lat = point.get("lat")
                if lon is None or lat is None:
                    cur.execute(
                        """
                        INSERT INTO geocode_cache(query_text, status, provider)
                        VALUES (%s, 'NOT_FOUND', '2gis')
                        ON CONFLICT (query_text) DO UPDATE
                          SET status='NOT_FOUND', provider='2gis', updated_at=NOW()
                        """,
                        (q,),
                    )
                    conn.commit()
                    return None

                cur.execute(
                    """
                    INSERT INTO geocode_cache(query_text, lon, lat, status, provider)
                    VALUES (%s, %s, %s, 'OK', '2gis')
                    ON CONFLICT (query_text) DO UPDATE
                      SET lon=EXCLUDED.lon, lat=EXCLUDED.lat, status='OK', provider='2gis', updated_at=NOW()
                    """,
                    (q, float(lon), float(lat)),
                )
                conn.commit()
                return float(lon), float(lat)

            except Exception as e:
                last_err = e
                # небольшой backoff
                time.sleep(0.3 * (attempt + 1))
                continue

        cur.execute(
            """
            INSERT INTO geocode_cache(query_text, status, provider)
            VALUES (%s, 'ERROR', '2gis')
            ON CONFLICT (query_text) DO UPDATE
              SET status='ERROR', provider='2gis', updated_at=NOW()
            """,
            (q,),
        )
        conn.commit()
        return None

    finally:
        conn.close()