from pyspark.sql import Column
from pyspark.sql import functions as F

from src.settings import CENTER_LAT, CENTER_LON

EARTH_RADIUS_KM = 6371.0088
KM_PER_LAT_DEG = 110.574
KM_PER_LON_DEG_AT_EQUATOR = 111.320


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    p1 = F.radians(lat1)
    p2 = F.radians(lat2)
    dphi = p2 - p1
    dlambda = F.radians(lon2) - F.radians(lon1)
    a = F.pow(F.sin(dphi / 2.0), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dlambda / 2.0), 2)
    return F.lit(2.0 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def distance_from_center_km(lat: Column, lon: Column) -> Column:
    return haversine_km(lat, lon, F.lit(CENTER_LAT), F.lit(CENTER_LON))


def local_x_km(lon: Column) -> Column:
    scale = KM_PER_LON_DEG_AT_EQUATOR * F.cos(F.radians(F.lit(CENTER_LAT)))
    return (lon - F.lit(CENTER_LON)) * scale


def local_y_km(lat: Column) -> Column:
    return (lat - F.lit(CENTER_LAT)) * F.lit(KM_PER_LAT_DEG)
