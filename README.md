# US Hotel Geocoder (NO STAGING) — FastAPI

This service updates `ingestion.hotel_masterfile` **directly** (no staging tables).

## What it does (US now)
- **missing_state_with_coords**: reverse geocode lat/lng -> get state name -> map using `test.us_states` -> update `state_code`
- **missing_latlng**: forward geocode address -> update `latitude/longitude` (+ state_code if resolved)

## Setup

```bash
python -m venv venv
source venv/bin/activate  # windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# edit DATABASE_URL + MASTERFILE_PK and column names if needed
uvicorn app.main:app --reload --port 8088
```

## Run

Stats:
`GET http://localhost:8088/geocode/us/stats`

Reverse geocode (fill missing state_code):
`GET http://localhost:8088/geocode/us/run?mode=missing_state_with_coords&limit=200&max_seconds=25`

Forward geocode (fill missing lat/lng):
`GET http://localhost:8088/geocode/us/run?mode=missing_latlng&limit=200&max_seconds=25`

Dry run (no commit):
`GET http://localhost:8088/geocode/us/run?mode=missing_latlng&commit=false`
or set `DRY_RUN=true` in `.env`.

## Future-proof note (international)
To extend internationally later:
- add `country_code` parameter
- use forward/reverse similarly
- **do not** map via `us_states`; store region/admin separately (or new columns)
