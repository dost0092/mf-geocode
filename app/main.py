from fastapi import FastAPI
from app.routes.geocode import router as geocode_router

app = FastAPI(title="US Hotel Geocoder (No Staging)", version="1.0.0")
app.include_router(geocode_router)

@app.get("/")
def root():
    return {
        "service": "us-hotel-geocoder-no-staging",
        "docs": "/docs",
        "health": "/geocode/health",
        "run": "/geocode/us/run",
        "stats": "/geocode/us/stats",
    }
