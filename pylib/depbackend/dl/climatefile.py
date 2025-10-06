""".. title:: DEP Climate File by Lat/Lon Point

This service provides a download of a climate file for a given latitude and
longitude point.  The service will search for the nearest climate file to the
requested point and provide it for download.

Changelog
---------

- 2025-08-28: Emit climate files for the Europe domain when requested.

"""

import os
from io import StringIO

import pandas as pd
from pydantic import Field
from pydep.io.wepp import read_cli
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.exceptions import NoDataFound
from pyiem.iemre import get_domain
from pyiem.util import logger
from pyiem.webutil import CGIModel, ListOrCSVType, iemapp
from sqlalchemy.engine import Connection

LOG = logger()


class Schema(CGIModel):
    """See how we are called."""

    lat: float = Field(..., description="Latitude of point", ge=-90, le=90)
    lon: float = Field(..., description="Longitude of point", ge=-180, le=180)
    format: str = Field("wepp", description="Output format, wepp or ntt")
    intensity: ListOrCSVType = Field(
        None, description="Comma delimited list of intensities to compute"
    )


def log_request(conn: Connection, environ: dict, fn: str, distance: float):
    """Log this request"""
    conn.execute(
        sql_helper("""
INSERT into clifile_requests(client_addr, geom, climate_file_id,
distance_degrees) VALUES (:addr, ST_Point(:lon, :lat, 4326),
(select id from climate_files where scenario = 0 and filepath = :fn),
:dist)
"""),
        {
            "lon": environ["lon"],
            "lat": environ["lat"],
            "fn": fn,
            "dist": distance,
            "addr": environ.get("REMOTE_ADDR"),
        },
    )
    conn.commit()


def find_closest_file(conn: Connection, lon: float, lat: float) -> tuple:
    """Find the closest climate file to the given point."""
    res = conn.execute(
        sql_helper("""
select filepath, st_distance(geom, ST_Point(:lon, :lat, 4326)) as distance
from climate_files where scenario = 0 and
ST_Contains(ST_MakeEnvelope(:west, :south, :east, :north, 4326), geom)
order by geom <-> ST_Point(:lon, :lat, 4326) asc limit 1
                """),
        {
            "lon": lon,
            "lat": lat,
            "west": lon - 1,
            "south": lat - 1,
            "east": lon + 1,
            "north": lat + 1,
        },
    )
    if res.rowcount == 0:
        return None, None
    row = res.first()
    return row[0], row[1]


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Go Main Go."""
    fmt = environ["format"]
    lon = environ["lon"]
    lat = environ["lat"]
    domain = get_domain(lon, lat)
    if domain is None or domain not in ["", "europe"]:
        raise NoDataFound("Point is outside of our domain")
    dbname = "idep" if domain == "" else f"dep_{domain}"
    with get_sqlalchemy_conn(dbname) as conn:
        fn, distance = find_closest_file(conn, lon, lat)
        if fn is None:
            raise NoDataFound("No climate files found in our database")
        if fmt == "wepp":
            # Log this request
            try:
                log_request(conn, environ, fn, distance)
            except Exception as exp:
                LOG.exception(exp)

    dlfn = os.path.basename(fn)
    if fmt == "ntt":
        dlfn = dlfn[:-4].replace(".", "_") + ".wth"
    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", f"attachment; filename={dlfn}"),
    ]
    start_response("200 OK", headers)
    if environ["intensity"]:
        levels = [int(x) for x in environ["intensity"]]
        df = read_cli(fn, compute_intensity_over=levels)
        df.index.name = "date"
        df = df.loc[: pd.Timestamp("now")]
        df = df[df["pcpn"] > 0]
        sio = StringIO()
        cols = [
            "pcpn",
        ] + [f"i{x}_mm" for x in levels]
        df[cols].to_csv(sio, float_format="%.2f")
        return sio.getvalue()

    if not os.path.isfile(fn):
        raise NoDataFound(f"Database found a file `{fn}` that does not exist")
    if fmt == "wepp":
        with open(fn, "rb") as fh:
            payload = fh.read()
    elif fmt == "ntt":
        df = read_cli(fn)
        payload = StringIO()
        # Convert langleys to MJ
        df["rad"] = df["rad"] * 0.04184
        for dt, row in df.iterrows():
            payload.write(
                f"  {dt.strftime('%Y %-2m %-2d')}  {row['rad']:3.0f}"
                f"{row['tmax']:6.1f} {row['tmin']:6.1f} {row['pcpn']:6.2f}"
                "\r\n"
            )
        payload = payload.getvalue()
    return payload
