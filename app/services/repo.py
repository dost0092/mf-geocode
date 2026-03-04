from typing import List, Dict, Any
from sqlalchemy import text
from app.config.settings import settings


def fetch_us_missing_state_with_coords(db, limit: int) -> List[Dict[str, Any]]:
    s = settings
    q = text(f'''
        SELECT *
        FROM {s.masterfile_schema}.{s.masterfile_table}
        WHERE {s.col_country_code} = 'US'
          AND ({s.col_state_code} IS NULL OR {s.col_state_code} = '')
          AND {s.col_lat} IS NOT NULL AND {s.col_lng} IS NOT NULL
          AND {s.col_lat} <> 0 AND {s.col_lng} <> 0
        ORDER BY {s.masterfile_pk}
        LIMIT :limit
    ''')
    return [dict(r) for r in db.execute(q, {"limit": limit}).mappings().all()]


def fetch_us_missing_latlng(db, limit: int) -> List[Dict[str, Any]]:
    s = settings
    q = text(f'''
        SELECT *
        FROM {s.masterfile_schema}.{s.masterfile_table}
        WHERE {s.col_country_code} = 'US'
          AND (
            {s.col_lat} IS NULL OR {s.col_lng} IS NULL
            OR {s.col_lat} = 0 OR {s.col_lng} = 0
          )
        ORDER BY {s.masterfile_pk}
        LIMIT :limit
    ''')
    return [dict(r) for r in db.execute(q, {"limit": limit}).mappings().all()]


def update_state_code(db, pk_value: Any, state_code: str) -> None:
    """
    Update state_code for a single hotel masterfile row.
    """
    s = settings
    q = text(f'''
        UPDATE {s.masterfile_schema}.{s.masterfile_table}
        SET {s.col_state_code} = :state_code
        WHERE {s.masterfile_pk} = :pk_value
    ''')
    db.execute(q, {"state_code": state_code, "pk_value": pk_value})


def update_latlng_and_state(
    db,
    pk_value: Any,
    lat: float,
    lng: float,
    state_code: str | None,
) -> None:
    """
    Update latitude, longitude, and optionally state_code for a single row.
    """
    s = settings
    q = text(f'''
        UPDATE {s.masterfile_schema}.{s.masterfile_table}
        SET {s.col_lat} = :lat,
            {s.col_lng} = :lng,
            {s.col_state_code} = COALESCE(:state_code, {s.col_state_code})
        WHERE {s.masterfile_pk} = :pk_value
    ''')
    db.execute(
        q,
        {"lat": lat, "lng": lng, "state_code": state_code, "pk_value": pk_value},
    )


def update_slug(db, pk_value: Any, slug: str) -> None:
    """
    Update slug for a single hotel masterfile row.
    """
    s = settings
    q = text(f'''
        UPDATE {s.masterfile_schema}.{s.masterfile_table}
        SET slug = :slug
        WHERE {s.masterfile_pk} = :pk_value
    ''')
    db.execute(q, {"slug": slug, "pk_value": pk_value})
