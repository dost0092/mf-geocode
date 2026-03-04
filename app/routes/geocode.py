from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.config.settings import settings
from app.services.pipeline import run_us_missing_state_with_coords, run_us_missing_latlng

router = APIRouter(prefix="/geocode", tags=["geocode"])

@router.get("/health")
def health():
    return {"ok": True}

@router.get("/us/run")
def run_us(
    mode: str = Query("missing_state_with_coords", description="missing_state_with_coords | missing_latlng"),
    limit: int = Query(None, ge=1, le=5000),
    max_seconds: int = Query(None, ge=5, le=120),
    commit: bool = Query(True, description="If false, rolls back after processing"),
    db: Session = Depends(get_db),
):
    limit = limit or settings.default_limit
    max_seconds = max_seconds or settings.default_max_seconds

    if mode == "missing_state_with_coords":
        summary = run_us_missing_state_with_coords(db, limit=limit, max_seconds=max_seconds)
    elif mode == "missing_latlng":
        summary = run_us_missing_latlng(db, limit=limit, max_seconds=max_seconds)
    else:
        return {"error": f"unknown mode: {mode}"}

    if settings.dry_run or not commit:
        db.rollback()
        summary["committed"] = False
        summary["dry_run"] = True if settings.dry_run else False
        return summary

    db.commit()
    summary["committed"] = True
    return summary

@router.get("/us/stats")
def us_stats(db: Session = Depends(get_db)):
    s = settings
    q = f'''
        SELECT
          COUNT(*) FILTER (WHERE {s.col_country_code}='US') AS total_us,
          COUNT(*) FILTER (WHERE {s.col_country_code}='US' AND {s.col_lat} IS NOT NULL AND {s.col_lng} IS NOT NULL AND {s.col_lat}<>0 AND {s.col_lng}<>0) AS have_latlng,
          COUNT(*) FILTER (WHERE {s.col_country_code}='US' AND ({s.col_lat} IS NULL OR {s.col_lng} IS NULL OR {s.col_lat}=0 OR {s.col_lng}=0)) AS missing_latlng,
          COUNT(*) FILTER (WHERE {s.col_country_code}='US' AND ({s.col_state_code} IS NULL OR {s.col_state_code}='')) AS missing_state
        FROM {s.masterfile_schema}.{s.masterfile_table};
    '''
    row = db.execute(q).mappings().first()
    return dict(row) if row else {}
