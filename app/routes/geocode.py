from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.config.settings import settings
from app.services.pipeline import run_us_missing_state_with_coords, run_us_missing_latlng
from app.services.geocoders.nominatim import NominatimGeocoder
import asyncio

router = APIRouter(prefix="/geocode", tags=["geocode"])

# Create a geocoder instance (with Nominatim → Geoapify → OpenCage fallback)
geocoder = NominatimGeocoder()

@router.get("/health")
def health():
    return {"ok": True}


async def async_run_us(mode: str, limit: int, max_seconds: int, commit: bool):
    """
    Run geocoding asynchronously in batches.
    """
    batch_size = 100  # adjust for your APIs
    remaining = limit

    # Use a DB session per batch
    from app.core.db import SessionLocal
    while remaining > 0:
        batch_limit = min(batch_size, remaining)
        with SessionLocal() as db:
            if mode == "missing_state_with_coords":
                rows = run_us_missing_state_with_coords(db, limit=batch_limit, max_seconds=max_seconds)
            elif mode == "missing_latlng":
                rows = run_us_missing_latlng(db, limit=batch_limit, max_seconds=max_seconds)
            else:
                break

            # Optionally commit or rollback
            if commit:
                db.commit()
            else:
                db.rollback()

        remaining -= batch_limit
        await asyncio.sleep(0.5)  # small pause between batches


@router.get("/us/run")
async def run_us(
    background_tasks: BackgroundTasks,
    mode: str = Query("missing_state_with_coords", description="missing_state_with_coords | missing_latlng"),
    limit: int = Query(None, ge=1, le=5000),
    max_seconds: int = Query(None, ge=5, le=120),
    commit: bool = Query(True, description="If false, rolls back after processing"),
):
    """
    Trigger geocoding in background.
    """
    limit = limit or settings.default_limit
    max_seconds = max_seconds or settings.default_max_seconds

    # Run as background task
    background_tasks.add_task(async_run_us, mode, limit, max_seconds, commit)

    return {
        "status": "started",
        "mode": mode,
        "limit": limit,
        "max_seconds": max_seconds,
        "message": "Geocoding is running in the background. No need to hit again."
    }


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
