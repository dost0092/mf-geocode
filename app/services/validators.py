import math
from typing import Dict, Any, Tuple

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * r * math.asin(math.sqrt(a))

def coord_ok(lat, lng) -> bool:
    if lat is None or lng is None:
        return False
    if lat == 0 or lng == 0:
        return False
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return False
    return True

def validate_candidate(original_row: Dict[str, Any], cand_lat: float, cand_lng: float, max_move_km: float) -> Tuple[bool, str]:
    if not coord_ok(cand_lat, cand_lng):
        return False, "bad_candidate_coords"

    old_lat = original_row.get("latitude")
    old_lng = original_row.get("longitude")
    if coord_ok(old_lat, old_lng):
        km = haversine_km(float(old_lat), float(old_lng), float(cand_lat), float(cand_lng))
        if km > max_move_km:
            return False, f"too_far_move_{km:.1f}km"

    return True, "ok"
