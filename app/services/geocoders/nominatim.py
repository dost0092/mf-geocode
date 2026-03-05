from typing import Optional, Dict, Any
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from opencage.geocoder import OpenCageGeocode

from app.config.settings import settings
from app.core.rate_limiter import RateLimiter
from .base import Geocoder


class NominatimGeocoder(Geocoder):

    name = "nominatim"

    def __init__(self):

        self.base = settings.nominatim_base_url.rstrip("/")

        self.client = httpx.Client(
            timeout=20.0,
            headers={"User-Agent": settings.nominatim_user_agent},
        )

        self.limiter = RateLimiter(settings.nominatim_rps)

        # Geoapify
        self.geoapify_key = "b0e126d67a9248d5813ca1a419c079e6"
        self.geoapify_base = "https://api.geoapify.com/v1"

        # OpenCage
        self.opencage_key = "257d3b15cc1646fc8e6f09078d1603c5"
        self.opencage = OpenCageGeocode(self.opencage_key)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def _get(self, url: str, params: dict):

        self.limiter.wait()

        print(f"[HTTP] {url} params={params}")

        r = self.client.get(url, params=params)

        print(f"[HTTP] status={r.status_code}")

        if r.status_code != 200:
            print(f"[HTTP] body={r.text}")

        r.raise_for_status()

        return r.json()

    # -------------------------
    # NOMINATIM
    # -------------------------

    def _nominatim_forward(self, query, country_code):

        params = {
            "q": query,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 1,
        }

        if country_code:
            params["countrycodes"] = country_code.lower()

        data = self._get(f"{self.base}/search", params)

        if not data:
            return None

        return data[0]

    def _nominatim_reverse(self, lat, lng):

        params = {
            "lat": lat,
            "lon": lng,
            "format": "jsonv2",
            "addressdetails": 1,
            "zoom": 10,
            "email": "waqasdost@gmail.com",
        }

        data = self._get(f"{self.base}/reverse", params)

        if not data or "error" in data:
            return None

        return data

    # -------------------------
    # GEOAPIFY
    # -------------------------

    def _geoapify_forward(self, query):

        params = {
            "text": query,
            "limit": 1,
            "apiKey": self.geoapify_key,
        }

        data = self._get(f"{self.geoapify_base}/geocode/search", params)

        features = data.get("features")

        if not features:
            return None

        return features[0]

    def _geoapify_reverse(self, lat, lng):

        params = {
            "lat": lat,
            "lon": lng,
            "apiKey": self.geoapify_key,
        }

        data = self._get(f"{self.geoapify_base}/geocode/reverse", params)

        features = data.get("features")

        if not features:
            return None

        return features[0]

    # -------------------------
    # OPENCAGE
    # -------------------------

    def _opencage_forward(self, query):

        results = self.opencage.geocode(query)

        if not results:
            return None

        return results[0]

    def _opencage_reverse(self, lat, lng):

        results = self.opencage.reverse_geocode(lat, lng)

        if not results:
            return None

        return results[0]

    # -------------------------
    # PUBLIC METHODS
    # -------------------------

    def forward(self, query: str, country_code: Optional[str] = None):

        if not query or not query.strip():
            return None

        # 1️⃣ Nominatim
        try:
            result = self._nominatim_forward(query, country_code)
            if result:
                print("✔ Nominatim success")
                return result
        except Exception as e:
            print("✖ Nominatim failed:", e)

        # 2️⃣ Geoapify
        try:
            result = self._geoapify_forward(query)
            if result:
                print("✔ Geoapify success")
                return result
        except Exception as e:
            print("✖ Geoapify failed:", e)

        # 3️⃣ OpenCage
        try:
            result = self._opencage_forward(query)
            if result:
                print("✔ OpenCage success")
                return result
        except Exception as e:
            print("✖ OpenCage failed:", e)

        return None

    def reverse(self, lat: float, lng: float):

        # 1️⃣ Nominatim
        try:
            result = self._nominatim_reverse(lat, lng)
            if result:
                print("✔ Nominatim reverse success")
                return result
        except Exception as e:
            print("✖ Nominatim reverse failed:", e)

        # 2️⃣ Geoapify
        try:
            result = self._geoapify_reverse(lat, lng)
            if result:
                print("✔ Geoapify reverse success")
                return result
        except Exception as e:
            print("✖ Geoapify reverse failed:", e)

        # 3️⃣ OpenCage
        try:
            result = self._opencage_reverse(lat, lng)
            if result:
                print("✔ OpenCage reverse success")
                return result
        except Exception as e:
            print("✖ OpenCage reverse failed:", e)

        return None
