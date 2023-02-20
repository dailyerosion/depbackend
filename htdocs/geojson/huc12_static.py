"""Answer /geojson/huc12.geojson with static metadata."""

from pymemcache.client import Client
import geopandas as gpd
from pyiem.util import get_sqlalchemy_conn


def do():
    """Do work"""
    with get_sqlalchemy_conn("idep") as conn:
        df = gpd.read_postgis(
            """
            SELECT ST_ReducePrecision(ST_Transform(simple_geom, 4326),
            0.0001) as geo, dominant_tillage as dt,
            round(average_slope_ratio::numeric, 3) as slp,
            huc_12, name from huc12 WHERE scenario = 0
            """,
            conn,
            index_col="huc_12",
            geom_col="geo",
        )
    return df.to_json()


def application(_environ, start_response):
    """Do Fun things"""
    headers = [("Content-Type", "application/vnd.geo+json")]
    start_response("200 OK", headers)
    mckey = "/geojson/huc12.geojson"
    mc = Client("iem-memcached:11211")
    res = mc.get(mckey)
    if res is None:
        res = do()
        mc.set(mckey, res, 86400)
    else:
        res = res.decode("utf-8")
    mc.close()
    return [res.encode("ascii")]
