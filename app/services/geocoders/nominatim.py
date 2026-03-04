from typing import Optional, Dict, Any
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config.settings import settings
from app.core.rate_limiter import RateLimiter
from .base import Geocoder

class NominatimGeocoder(Geocoder):
    name = "nominatim"

    def __init__(self):
        self.base = settings.nominatim_base_url.rstrip("/")
        self.client = httpx.Client(timeout=20.0, headers={"User-Agent": settings.nominatim_user_agent})
        self.limiter = RateLimiter(settings.nominatim_rps)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def _get(self, path: str, params: dict) -> dict:
        self.limiter.wait()
        r = self.client.get(f"{self.base}{path}", params=params)
        r.raise_for_status()
        return r.json()

    def forward(self, query: str, country_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not query or not query.strip():
            return None
        params = {
            "q": query,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 1,
        }
        if country_code:
            params["countrycodes"] = country_code.lower()
        data = self._get("/search", params)
        if not data:
            return None
        return data[0]

    def reverse(self, lat: float, lng: float) -> Optional[Dict[str, Any]]:
        params = {
            "lat": lat,
            "lon": lng,
            "format": "jsonv2",
            "addressdetails": 1,
            "zoom": 18,
        }
        data = self._get("/reverse", params)
        if not data or "error" in data:
            return None
        return data
