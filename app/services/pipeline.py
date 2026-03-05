from typing import Dict, Any
import logging
import os
import re
import time

from app.config.settings import settings
from app.services.geocoders.nominatim import NominatimGeocoder
from app.services.state_service import normalize_state_code
from app.services.validators import validate_candidate
from app.services import repo


logger = logging.getLogger("geocode_updates")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler("logs/geocode_updates.log", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


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
# -------------------------------------------------
# COORDINATE EXTRACTOR (works for all providers)
# -------------------------------------------------

def extract_lat_lng(payload):

    if not payload:
        return None, None

    # Nominatim
    if "lat" in payload and "lon" in payload:
        return float(payload["lat"]), float(payload["lon"])

    # Geoapify
    if "properties" in payload:
        props = payload["properties"]
        if "lat" in props and "lon" in props:
            return float(props["lat"]), float(props["lon"])

    # OpenCage
    if "geometry" in payload:
        geo = payload["geometry"]
        if "lat" in geo and "lng" in geo:
            return float(geo["lat"]), float(geo["lng"])

    return None, None

def _strip_component(value: str | None) -> str:
    """Strip a slug component to a-z0-9, encoding non-ASCII as UTF-8 hex."""
    if not value:
        return "null"

    result = ""
    for c in value.lower():
        if re.match(r"[a-z0-9]", c):
            result += c
        elif ord(c) > 127:
            result += c.encode("utf-8").hex()

    return result or "null"


def generate_kruiz_slug(hotel_name, country=None, state=None, city=None, address=None):
    return "-".join([
        _strip_component(country),
        _strip_component(state),
        _strip_component(city),
        _strip_component(hotel_name),
        _strip_component(address),
    ])


def build_slug(row):
    """
    Build slug using Kruizy format:
    country-state-city-hotelname-address
    """

    hotel_name = row.get("name")
    country = row.get(settings.col_country_code)
    state = row.get(settings.col_state_code)
    city = row.get(settings.col_city)

    # address line
    address = row.get(settings.col_address1)

    return generate_kruiz_slug(
        hotel_name,
        country,
        state,
        city,
        address
    )

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
    updated_ids: list[str] = []
    start = time.monotonic()

    print(f"[Tier1A] starting batch: {len(rows)} rows, limit={limit}, max_seconds={max_seconds}")
    logger.info("tier=1A mode=missing_state_with_coords start selected=%s", len(rows))

    for row in rows:
        if time.monotonic() - start > max_seconds:
            break

        pk_value = row[settings.masterfile_pk]
        try:
            lat = float(row[settings.col_lat])
            lng = float(row[settings.col_lng])

            print(f"[Tier1A] reverse request lat={lat} lng={lng}")

            payload = geocoder.reverse(lat, lng)
            time.sleep(1.2)

            print(f"[Tier1A] reverse response={payload}")

            if not payload:
                print(f"[Tier1A] FAILED reverse geocode id={pk_value}")
                failed += 1
                processed += 1
                continue

            state_name = extract_state_name(payload)

            print(f"[Tier1A] state_name={state_name}")

            state_code = normalize_state_code(db, state_name)

            print(f"[Tier1A] state_code={state_code}")
            if not state_code:
                failed += 1
                processed += 1
                continue

            repo.update_state_code(db, pk_value, state_code)

            row_with_new_state = dict(row)
            row_with_new_state[settings.col_state_code] = state_code
            print("ROW:", row_with_new_state)
            print("STATE:", row_with_new_state.get(settings.col_state_code))
            slug = build_slug(row_with_new_state)
            repo.update_slug(db, pk_value, slug)

            print(f"[Tier1A] updated id={pk_value} state_code={state_code} slug={slug}")
            logger.info(
                "tier=1A mode=missing_state_with_coords updated id=%s state_code=%s slug=%s",
                pk_value,
                state_code,
                slug,
            )

            updated += 1
            updated_ids.append(str(pk_value))
            processed += 1
        except Exception:
            failed += 1
            processed += 1

    summary = {
        "mode": "missing_state_with_coords",
        "selected": len(rows),
        "processed": processed,
        "updated": updated,
        "failed": failed,
        "stopped_by_time": (time.monotonic() - start > max_seconds),
    }

    print(f"[Tier1A] summary={summary} updated_ids={updated_ids}")
    logger.info(
        "tier=1A mode=missing_state_with_coords summary=%s updated_ids=%s",
        summary,
        updated_ids,
    )

    return summary


def run_us_missing_latlng(db, limit: int, max_seconds: int) -> Dict[str, Any]:
    """
    Tier 1B (US): forward geocode rows that are missing or have bad coordinates.
    """
    geocoder = NominatimGeocoder()
    rows = repo.fetch_us_missing_latlng(db, limit=limit)

    processed = 0
    updated = 0
    failed = 0
    updated_ids: list[str] = []
    start = time.monotonic()

    print(f"[Tier1B] starting batch: {len(rows)} rows, limit={limit}, max_seconds={max_seconds}")
    logger.info("tier=1B mode=missing_latlng start selected=%s", len(rows))

    for row in rows:
        if time.monotonic() - start > max_seconds:
            break

        pk_value = row[settings.masterfile_pk]

        try:
            query = build_us_address(row)
            payload = geocoder.forward(query, country_code="US")
            if not payload:
                failed += 1
                processed += 1
                continue

            cand_lat, cand_lng = extract_lat_lng(payload)

            print(f"[Tier1B] forward response lat={cand_lat} lng={cand_lng}")
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

            repo.update_latlng_and_state(db, pk_value, cand_lat, cand_lng, state_code)

            row_with_new_values = dict(row)
            row_with_new_values[settings.col_lat] = cand_lat
            row_with_new_values[settings.col_lng] = cand_lng
            if state_code:
                row_with_new_values[settings.col_state_code] = state_code

            slug = build_slug(row_with_new_values)
            repo.update_slug(db, pk_value, slug)

            print(
                f"[Tier1B] updated id={pk_value} "
                f"lat={cand_lat} lng={cand_lng} state_code={state_code} slug={slug}"
            )
            logger.info(
                "tier=1B mode=missing_latlng updated id=%s lat=%s lng=%s state_code=%s slug=%s",
                pk_value,
                cand_lat,
                cand_lng,
                state_code,
                slug,
            )

            updated += 1
            updated_ids.append(str(pk_value))
            processed += 1
        except Exception:
            failed += 1
            processed += 1

    summary = {
        "mode": "missing_latlng",
        "selected": len(rows),
        "processed": processed,
        "updated": updated,
        "failed": failed,
        "stopped_by_time": (time.monotonic() - start > max_seconds),
    }

    print(f"[Tier1B] summary={summary} updated_ids={updated_ids}")
    logger.info(
        "tier=1B mode=missing_latlng summary=%s updated_ids=%s",
        summary,
        updated_ids,
    )

    return summary


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
