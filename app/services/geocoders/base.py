from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class Geocoder(ABC):
    name: str = "base"

    @abstractmethod
    def forward(self, query: str, country_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    def reverse(self, lat: float, lng: float) -> Optional[Dict[str, Any]]:
        ...
