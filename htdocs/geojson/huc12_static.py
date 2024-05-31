"""Answer /geojson/huc12.geojson with static metadata."""

import geopandas as gpd
from pyiem.database import get_sqlalchemy_conn
from pyiem.webutil import iemapp


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


@iemapp(
    content_type="application/vnd.geo+json",
    memcachekey="/geojson/huc12.geojson",
    memcacheexpire=86400,
)
def application(_environ, start_response):
    """Do Fun things"""
    headers = [("Content-Type", "application/vnd.geo+json")]
    start_response("200 OK", headers)
    return do()
