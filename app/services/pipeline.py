from typing import Dict, Any
import time

from app.config.settings import settings
from app.services.geocoders.nominatim import NominatimGeocoder
from app.services.state_service import normalize_state_code
from app.services.validators import validate_candidate
from app.services import repo


def build_us_address(row: Dict[str, Any]) -> str:
    """
    Build a simple US address string from a masterfile row.

    This is used by Tier 1 (US) forward geocoding.
    """
    s = settings
    parts = []

    if row.get(s.col_address1):
        parts.append(str(row[s.col_address1]))
    if row.get(s.col_city):
        parts.append(str(row[s.col_city]))

    # Prefer existing state_code; fall back to state text
    if row.get(s.col_state_code):
        parts.append(str(row[s.col_state_code]))
    elif row.get(s.col_state_text):
        parts.append(str(row[s.col_state_text]))

    if row.get(s.col_postal):
        parts.append(str(row[s.col_postal]))

    parts.append("USA")
    return ", ".join(p for p in parts if p and str(p).strip())


def extract_state_name(payload: Dict[str, Any]) -> str | None:
    """
    Extract a best-effort state/region name from a geocoder payload.
    """
    addr = payload.get("address") or payload.get("addressdetails") or {}
    return addr.get("state") or addr.get("region") or addr.get("province")


def run_us_missing_state_with_coords(db, limit: int, max_seconds: int) -> Dict[str, Any]:
    """
    Tier 1A (US): reverse geocode rows that have coordinates but no state_code.
    """
    geocoder = NominatimGeocoder()
    rows = repo.fetch_us_missing_state_with_coords(db, limit=limit)

    processed = 0
    updated = 0
    failed = 0
    start = time.monotonic()

    for row in rows:
        if time.monotonic() - start > max_seconds:
            break

        hotel_id = int(row[settings.masterfile_pk])
        try:
            payload = geocoder.reverse(float(row[settings.col_lat]), float(row[settings.col_lng]))
            if not payload:
                failed += 1
                processed += 1
                continue

            state_name = extract_state_name(payload)
            state_code = normalize_state_code(db, state_name)
            if not state_code:
                failed += 1
                processed += 1
                continue

            repo.update_state_code(db, hotel_id, state_code)
            updated += 1
            processed += 1
        except Exception:
            failed += 1
            processed += 1

    return {
        "mode": "missing_state_with_coords",
        "selected": len(rows),
        "processed": processed,
        "updated": updated,
        "failed": failed,
        "stopped_by_time": (time.monotonic() - start > max_seconds),
    }


def run_us_missing_latlng(db, limit: int, max_seconds: int) -> Dict[str, Any]:
    """
    Tier 1B (US): forward geocode rows that are missing or have bad coordinates.
    """
    geocoder = NominatimGeocoder()
    rows = repo.fetch_us_missing_latlng(db, limit=limit)

    processed = 0
    updated = 0
    failed = 0
    start = time.monotonic()

    for row in rows:
        if time.monotonic() - start > max_seconds:
            break

        hotel_id = int(row[settings.masterfile_pk])

        try:
            query = build_us_address(row)
            payload = geocoder.forward(query, country_code="US")
            if not payload:
                failed += 1
                processed += 1
                continue

            cand_lat = float(payload.get("lat")) if payload.get("lat") is not None else None
            cand_lng = float(payload.get("lon")) if payload.get("lon") is not None else None
            if cand_lat is None or cand_lng is None:
                failed += 1
                processed += 1
                continue

            ok, _reason = validate_candidate(row, cand_lat, cand_lng, settings.max_move_km)
            if not ok:
                failed += 1
                processed += 1
                continue

            state_name = extract_state_name(payload)
            state_code = normalize_state_code(db, state_name)

            repo.update_latlng_and_state(db, hotel_id, cand_lat, cand_lng, state_code)
            updated += 1
            processed += 1
        except Exception:
            failed += 1
            processed += 1

    return {
        "mode": "missing_latlng",
        "selected": len(rows),
        "processed": processed,
        "updated": updated,
        "failed": failed,
        "stopped_by_time": (time.monotonic() - start > max_seconds),
    }


def run_tier1_us_missing_state_with_coords(db, limit: int, max_seconds: int) -> Dict[str, Any]:
    """
    Tier 1 wrapper for US hotels (missing state but have coordinates).

    This keeps the current behavior but makes the tier explicit in code.
    """
    return run_us_missing_state_with_coords(db, limit=limit, max_seconds=max_seconds)


def run_tier1_us_missing_latlng(db, limit: int, max_seconds: int) -> Dict[str, Any]:
    """
    Tier 1 wrapper for US hotels (missing or bad coordinates).

    This keeps the current behavior but makes the tier explicit in code.
    """
    return run_us_missing_latlng(db, limit=limit, max_seconds=max_seconds)


def run_tier2_international_missing_latlng(db, limit: int, max_seconds: int) -> Dict[str, Any]:
    """
    Tier 2 (international) stub.

    Intentionally not implemented yet; this is a placeholder to keep the code
    future-proof without changing existing Tier 1 behavior.
    """
    return {
        "mode": "tier2_international_missing_latlng",
        "selected": 0,
        "processed": 0,
        "updated": 0,
        "failed": 0,
        "stopped_by_time": False,
        "implemented": False,
    }
